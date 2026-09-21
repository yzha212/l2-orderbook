"""Write the raw Coinbase frames to disk exactly as they arrived.

This is stage 8 of the architecture. It reads l2.raw.btc-usd and gathers frames into one file
per quarter hour, so the frames that only live for a day on the topic can still be replayed in
a year. They are the only source the book can be rebuilt from: everything else in this project,
the published book and its Parquet archive alike, is derived from these bytes and can be made
again from them. Nothing here can be made again from anything else.

Newline-delimited JSON rather than the Parquet stage 7 writes, because this stage must not
parse. A frame that stage 3 refuses is exactly the frame worth keeping, and a typed schema has
nowhere to put one; a column layout would also force a decode of every frame to fill it. So the
frame is stored byte for byte and any question about its contents is answered at read time,
where parsing is cheap and a failure costs nothing. That is also why this is the one service
that imports neither src.decode nor src.book: the last line of defence should not break when
the exchange changes shape.

One line per frame:

    <arrival ISO 8601> <kafka offset> <frame exactly as the exchange sent it>

Split on the first two spaces to recover the frame. The two prefix fields answer two different
questions and the archive needs both. The exchange's own sequence_num, which is already inside
the frame, says whether the exchange sent something we never received. The offset says whether
our own pipeline carried something we failed to archive. Different faults, different fixes --
and the offset is the half that is lost for good once the topic's day of retention expires,
which is why it is lifted out here while sequence_num is left where it already sits.

A reader should treat the prefix as everything before the first field starting with "{", rather
than counting spaces, so that a field added later -- conn_id is already planned -- costs it
nothing. An empty line is reserved: it never carries a frame, so that it stays free to mark the
connect and disconnect events the architecture asks for once ingest publishes them.

Frames are written as they arrive rather than gathered in memory, because a single opening
snapshot runs to about 5 MB and a quarter hour holds one per reconnect. Only the bucket's file
name is decided in advance; the bytes go straight through the compressor. Nothing is committed
until that file has been closed, flushed to the disk and moved into place, so a crash rewinds to
the last commit and writes the bucket again rather than losing it.

Measured on the live feed rather than guessed at. One quarter hour held 6,307 frames, 12,305,799
bytes of JSON, and came to 1,037,060 bytes on disk: 164 bytes a frame, a little under twelve to
one, and around 100 MB a day at that rate. A frame averages about 1.9 kB, so the ratio comes from
how alike consecutive updates are rather than from any single frame compressing well. Bucket
length barely moves it -- five, fifteen and sixty minute buckets came out within 0.7% of each
other per frame -- because zstd looks back over a limited window and consecutive frames already
sit inside it. Fifteen minutes is chosen for the other reasons: it bounds how much is re-read
after a crash, and it matches stage 7 so the two archives line up file for file.
"""

import argparse
import os  # reads the broker list and the group name, and flushes files to the disk
import signal  # lets a stop request from Docker reach the loop below
import time
import types  # only for the stack frame a signal handler is handed
from compression.zstd import ZstdError, ZstdFile  # stdlib on 3.14; no dependency to pin
from datetime import UTC, datetime, timedelta
from pathlib import Path

from confluent_kafka import (  # librdkafka client
    TIMESTAMP_NOT_AVAILABLE,
    Consumer,
    KafkaError,
    KafkaException,
    TopicPartition,
)

RAW_TOPIC = "l2.raw.btc-usd"  # must match the topic `topic-init` creates in docker-compose.yml
PARTITION = 0  # the raw topic is created with exactly one; start_position refuses any other count
BROKERS = os.environ.get("REDPANDA_BROKERS", "redpanda-0:9092")  # host runs use localhost:19092
GROUP_ID = os.environ.get("CONSUMER_GROUP", "l2-raw-archive")  # the name ARCHITECTURE.md gives it
ARCHIVE_ROOT = Path(os.environ.get("ARCHIVE_ROOT", "raw"))  # /app/raw in the container
SYMBOL = "BTC-USD"  # the one symbol this project follows, and the partition key on disk
MAX_MESSAGE_BYTES = 20 * 1024 * 1024  # the opening snapshot runs ~5 MB, over the 1 MB default
BUCKET_MINUTES = 15  # one file per quarter hour, matching stage 7 so the two archives line up
BATCH_SIZE = 1000  # records asked for per fetch; this reader wants throughput, not freshness
POLL_TIMEOUT = 1.0  # seconds a fetch waits, which also paces the idle check and the progress line
IDLE_CLOSE_SECONDS = 60.0  # grace before a silent feed's last bucket is closed on our own clock
STATS_INTERVAL = 10.0  # seconds between progress lines


def build_consumer() -> Consumer:
    """Create the consumer used to read the raw frames back.

    Returns:
        A consumer configured to talk to the Redpanda broker.
    """
    return Consumer(  # config is a plain dict of librdkafka property names
        {
            "bootstrap.servers": BROKERS,  # where to find the cluster
            "group.id": GROUP_ID,  # a real group here: it holds the position and locks the writer
            "auto.offset.reset": "earliest",  # a group with no position starts at the oldest record
            # The rename is what earns a commit, never the clock. Auto-commit would move the
            # position on a timer while a bucket was still only half written, so a crash would
            # skip past frames that never reached a finished file.
            "enable.auto.commit": False,
            "fetch.message.max.bytes": MAX_MESSAGE_BYTES,  # must clear the snapshot frame's size
        }
    )


def stamp(moment: datetime) -> str:
    """Render an arrival time for the line prefix.

    Milliseconds rather than microseconds because that is all the broker's timestamp carries;
    printing six digits would invent three of them.

    Args:
        moment: The arrival time, in any zone.

    Returns:
        An ISO 8601 instant in UTC, ending in Z, that sorts in the order it happened.
    """
    utc = moment.astimezone(UTC)
    return f"{utc:%Y-%m-%dT%H:%M:%S}.{utc.microsecond // 1000:03d}Z"


def parse_prefix(line: bytes) -> tuple[datetime, int] | None:
    """Read back the arrival time and offset a line was written with.

    Used by the two guards below rather than by any hot path, so it favours being forgiving
    over being fast: anything it cannot make sense of is reported as nothing to compare.

    Args:
        line: One whole line as it was written, with or without its newline.

    Returns:
        The arrival time and offset, or None if this line does not start with the prefix.
    """
    try:  # EAFP: a short line or an unparsable field lands here rather than being measured first
        when, offset, _ = line.split(b" ", 2)
        moment = datetime.strptime(when.decode(), "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
        return moment, int(offset)
    except (ValueError, UnicodeDecodeError):
        return None


def bucket_start(moment: datetime, minutes: int) -> datetime:
    """Round a moment down to the start of the bucket it falls in.

    Args:
        moment: Any instant; converted to UTC first, so the caller's zone cannot shift it.
        minutes: Bucket length, which main() has already checked divides an hour evenly.

    Returns:
        The first instant of that bucket, in UTC.
    """
    utc = moment.astimezone(UTC)
    return utc.replace(minute=utc.minute - utc.minute % minutes, second=0, microsecond=0)


def path_for(root: Path, symbol: str, start: datetime) -> Path:
    """Work out where one bucket's file belongs.

    The same layout stage 7 uses, so the two archives line up: the raw frames behind any book
    file sit in the file with the same name under a different root. The directories carry the
    meaning and the file name carries only what changes between files, except for the date,
    which is repeated there so that files copied into one folder cannot quietly collide.

    Args:
        root: The archive root, `raw` unless the environment says otherwise.
        symbol: The product these frames are for.
        start: The first instant of the bucket, from bucket_start.

    Returns:
        The full path of the finished file, which may not exist yet.
    """
    return (
        root
        / f"symbol={symbol}"
        / f"date={start:%Y-%m-%d}"
        / f"{start:%Y%m%dT%H%M}.ndjson.zst"
    )


def first_line(path: Path) -> bytes | None:
    """Read the first frame line out of a finished file.

    Args:
        path: The file to read, which may not exist.

    Returns:
        The line, or None when the file is missing, unreadable or holds no frame lines.
    """
    try:  # EAFP: a missing or truncated file answers None rather than being checked for first
        with ZstdFile(path, "rb") as reader:
            for line in reader:
                if line.strip():  # skip the reserved empty lines
                    return line
    except (OSError, ZstdError):
        return None
    return None


def last_line(path: Path) -> bytes | None:
    """Read the last frame line out of a finished file.

    Walks the whole file, because a compressed stream cannot be read backwards. That costs one
    pass over a few megabytes, and it happens once at startup rather than per record.

    Args:
        path: The file to read, which may not exist.

    Returns:
        The line, or None when the file is missing, unreadable or holds no frame lines.
    """
    tail: bytes | None = None
    try:  # EAFP: a file cut short by a kill still yields the lines that were whole
        with ZstdFile(path, "rb") as reader:
            for line in reader:
                if line.strip():  # skip the reserved empty lines
                    tail = line
    except (OSError, ZstdError):
        return tail
    return tail


def newest_archive(root: Path) -> Path | None:
    """Find the most recent finished file in the archive.

    The date sits in both the folder name and the file name, so a plain sort over full paths is
    already chronological.

    Args:
        root: The archive root to look under.

    Returns:
        The newest file, or None when nothing has been archived yet.
    """
    files = sorted(root.glob("symbol=*/date=*/*.ndjson.zst"))
    return files[-1] if files else None


class Bucket:
    """One quarter hour of frames, written straight to its file as they arrive.

    Nothing is held in memory beyond the frame being written, which is what lets a 5 MB opening
    snapshot pass through without the bucket growing to match it. The file is written under a
    name nothing looks for and only moved into place once the bucket is whole, so a bucket
    dropped on a stop or a crash costs no more than reading those frames again.

    Attributes:
        start: The first instant of this bucket, which decides the file name.
        end: The first instant of the next bucket, used by the idle check.
        rows: Frames written so far.
        first_offset: Broker position of the first frame, for the range this file covers.
        last_offset: Broker position of the most recent frame, and what gets committed.
        first_arrival: When the first frame arrived, which the overwrite guard compares on.
        last_arrival: When the most recent frame arrived.
        offset_gaps: Places where the broker position jumped, which on a single-partition topic
            means a record was skipped or had already expired.
        undated: Frames whose record carried no usable broker timestamp.
        embedded_newlines: Frames whose own bytes held a newline, which breaks one frame per
            line. Written anyway, because losing the bytes is worse than an awkward file.
    """

    def __init__(self, start: datetime, minutes: int, root: Path, symbol: str) -> None:
        """Open an empty bucket, and the file it will be written to.

        Args:
            start: The first instant of the bucket, from bucket_start.
            minutes: Bucket length, which decides when it is over.
            root: The archive root.
            symbol: The product these frames are for.
        """
        self.start = start
        self.end = start + timedelta(minutes=minutes)
        self.final = path_for(root, symbol, start)
        self.part = self.final.with_name(f"{self.final.name}.part")
        self.final.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.part.open("wb")  # kept, so the fsync below has a file descriptor
        self.writer = ZstdFile(self.handle, "wb")  # wraps it; closing this does not close that
        self.rows = 0
        self.first_offset: int | None = None
        self.last_offset: int | None = None
        self.first_arrival: datetime | None = None
        self.last_arrival: datetime | None = None
        self.offset_gaps = 0
        self.undated = 0
        self.embedded_newlines = 0

    def add(self, offset: int, arrival: datetime, payload: bytes, dated: bool) -> None:
        """Write one frame, preceded by when it arrived and where it sat on the topic.

        Args:
            offset: The broker's position for this record.
            arrival: When the record reached the topic.
            payload: The frame exactly as the exchange sent it.
            dated: False when the record carried no usable timestamp and one was assumed.
        """
        if self.last_offset is not None and offset > self.last_offset + 1:
            self.offset_gaps += 1  # a hole in the topic, which the summary should say out loud
        if self.first_offset is None:
            self.first_offset = offset
            self.first_arrival = arrival
        self.last_offset = offset
        self.last_arrival = arrival
        self.undated += not dated
        if b"\n" in payload:  # should never happen; see the module docstring on framing
            self.embedded_newlines += 1
            print(f"[warn] offset {offset}: frame contains a newline and will span lines")
        # The prefix is written as one piece so that a frame can never be separated from it.
        self.writer.write(f"{stamp(arrival)} {offset} ".encode() + payload + b"\n")
        self.rows += 1

    def _seal(self) -> None:
        """Close the compressor and push every byte of the part file to the disk."""
        self.writer.close()  # flushes the final compressed block
        self.handle.flush()
        os.fsync(self.handle.fileno())  # the bytes, not just the buffer
        self.handle.close()

    def write_out(self) -> Path:
        """Finish the file and make it visible in a single step.

        Replacing a file that already exists is normal: it means the last run wrote this bucket
        and stopped before recording its position, so this run read the same frames again. The
        guard below is for the case where that assumption breaks. If the bucket now in hand
        began later than the file already on disk, this is not a rewrite of the same stretch --
        it is a shorter one about to erase a longer one, which is what a recreated topic does --
        so both are kept and the new one is named apart.

        The comparison is on arrival time rather than offset on purpose: offsets restart at zero
        when a topic is recreated, which is the very case being guarded against.

        Returns:
            The path the bucket was written to, which is the usual name unless the guard fired.
        """
        self._seal()
        final = self.final
        rival = parse_prefix(first_line(final) or b"")  # None when no file is there to compare
        if rival is not None and self.first_arrival is not None and self.first_arrival > rival[0]:
            final = final.with_name(
                f"{self.start:%Y%m%dT%H%M}.rewind-{self.first_arrival:%Y%m%dT%H%M%S}.ndjson.zst"
            )
            print(
                f"[warn] {self.final} already holds frames from {stamp(rival[0])} and this "
                f"bucket only starts at {stamp(self.first_arrival)}; keeping both, writing {final}"
            )
        self.part.replace(final)  # one operation, so a reader never sees a half-written file
        folder = os.open(final.parent, os.O_RDONLY)  # the move itself has to reach the disk too
        try:
            os.fsync(folder)
        finally:
            os.close(folder)
        return final

    def discard(self) -> None:
        """Throw away a bucket that was never finished, leaving nothing behind."""
        try:  # EAFP: a part file already gone is exactly the state wanted here
            self._seal()
            self.part.unlink()
        except (OSError, ValueError):
            pass


def sweep_parts(root: Path) -> None:
    """Delete any half-written file a killed run left behind.

    These are never read and are rubbish by definition: the run that was writing one never got
    as far as moving it into place, so whatever it holds is still on the topic.

    Args:
        root: The archive root to sweep.
    """
    for leftover in sorted(root.glob("symbol=*/date=*/*.ndjson.zst.part")):
        print(f"[swept] {leftover} was left half-written by an earlier run")
        leftover.unlink()


def start_position(consumer: Consumer) -> None:
    """Check the topic is shaped the way this writer assumes, and say where it resumes.

    Args:
        consumer: The consumer, already built but not yet reading.

    Raises:
        KafkaException: If the topic is missing, or no longer has one partition.
    """
    topic = consumer.list_topics(RAW_TOPIC, timeout=10.0).topics[RAW_TOPIC]
    if topic.error is not None:
        raise KafkaException(topic.error)
    if len(topic.partitions) != 1:  # the offset is a line's identity, which two partitions break
        raise KafkaException(
            f"{RAW_TOPIC} has {len(topic.partitions)} partitions; this writer needs exactly one"
        )
    spot = TopicPartition(RAW_TOPIC, PARTITION)
    committed = consumer.committed([spot], timeout=10.0)[0].offset
    low, high = consumer.get_watermark_offsets(spot, timeout=10.0, cached=False)
    if committed < 0:  # no position recorded yet, so auto.offset.reset decides where to begin
        print(f"[position] no position recorded for {GROUP_ID}; starting at {low}")
        return
    if committed < low:  # the one case where frames are gone for good
        print(
            f"[warn] {GROUP_ID} was at {committed} but the topic now starts at {low}: "
            f"{low - committed} frames expired before they were archived"
        )
        return
    print(f"[position] resuming {GROUP_ID} at {committed}, topic holds {low} to {high}")


def check_topic_rewound(consumer: Consumer, root: Path) -> None:
    """Say so loudly if the topic now holds fewer records than we have already archived.

    Deleting and recreating a topic sets its offsets back to zero and takes the group's stored
    position with it, so nothing else here would notice. The archive itself is the only memory
    that survives that, which is why this asks the newest file rather than the broker.

    Args:
        consumer: The consumer, used only to read the watermark.
        root: The archive root.
    """
    newest = newest_archive(root)
    if newest is None:  # nothing archived yet, so there is nothing to have gone backwards from
        return
    seen = parse_prefix(last_line(newest) or b"")
    if seen is None:
        return
    spot = TopicPartition(RAW_TOPIC, PARTITION)
    _, high = consumer.get_watermark_offsets(spot, timeout=10.0, cached=False)
    if high < seen[1]:  # the topic cannot have fewer records than we have already written down
        print(
            f"[warn] {newest.name} already holds offset {seen[1]} but {RAW_TOPIC} now ends at "
            f"{high}: the topic looks recreated, so offsets here repeat earlier ones"
        )


class Meter:
    """Counts for the progress line, so a long run says what it is doing.

    Attributes:
        read: Frames written since the run began.
        files: Finished files written.
        started: When the run began, for the closing summary.
    """

    def __init__(self) -> None:
        """Start every count at zero, from now."""
        self.read = 0
        self.files = 0
        self.started = time.monotonic()
        self.at = self.started + STATS_INTERVAL  # when the next line is owed

    def due(self) -> bool:
        """Say whether a progress line is owed, and book the next one if so.

        Returns:
            True when one interval has passed since the last line.
        """
        now = time.monotonic()
        if now < self.at:
            return False
        self.at = now + STATS_INTERVAL
        return True

    def report(self, consumer: Consumer, bucket: "Bucket | None") -> str:
        """Describe progress in one line.

        Args:
            consumer: The consumer, asked how far behind the topic it is.
            bucket: The bucket being gathered, or None between buckets.

        Returns:
            One line naming the read count, the lag, the files written and the bucket open.
        """
        elapsed = max(time.monotonic() - self.started, 1e-9)
        open_now = "none" if bucket is None else f"{bucket.start:%H:%M} rows {bucket.rows}"
        lag = lag_of(consumer)
        return (
            f"[stats] read {self.read} ({self.read / elapsed:.1f}/s) "
            f"lag {'?' if lag is None else lag} files {self.files} bucket {open_now}"
        )


def lag_of(consumer: Consumer) -> int | None:
    """Say how many records are still to read before the end of the topic.

    Uses the watermarks the client already has from its last fetch, so this costs no call to the
    broker and can sit both on the progress line and in the idle check after every fetch.

    Args:
        consumer: The consumer to ask.

    Returns:
        Records still to read, or None while there is no position or no watermark yet -- which
        is not the same as zero, and the caller has to tell the two apart.
    """
    try:  # EAFP: before the first fetch there is no position and no watermark to compare
        spot = consumer.position([TopicPartition(RAW_TOPIC, PARTITION)])[0]
        _, high = consumer.get_watermark_offsets(spot, cached=True)
        if spot.offset < 0 or high < 0:  # the client's stand-in for "not known yet"
            return None
        return max(high - spot.offset, 0)
    except (KafkaException, TypeError):
        return None


def close_bucket(consumer: Consumer, bucket: Bucket, meter: Meter) -> None:
    """Write a finished bucket to disk and only then record the position it reached.

    The order is the whole durability story. If this run dies between the write and the commit,
    the next one reads the same frames again and writes the same file over the top; if it died
    the other way round, those frames would be skipped for good.

    Args:
        consumer: The consumer whose position is recorded once the file is safe.
        bucket: The finished bucket.
        meter: Counts to update once the file has landed.
    """
    if not bucket.rows:  # an empty bucket has no file worth leaving and no position to record
        bucket.discard()
        print(f"[empty] {bucket.start:%Y-%m-%dT%H:%M} held no frames; nothing written")
        return
    final = bucket.write_out()
    faults = (
        f"gaps {bucket.offset_gaps}, undated {bucket.undated}, "
        f"newlines {bucket.embedded_newlines}"
    )
    print(
        f"[closed] {final} rows {bucket.rows} "
        f"offsets {bucket.first_offset}-{bucket.last_offset} "
        f"({final.stat().st_size} bytes, {faults})"
    )
    # Committed last, and synchronously, so a refusal is visible here rather than discovered as
    # a rewritten bucket much later. A refusal is not fatal: the file is already on disk and the
    # next run simply writes this bucket again.
    try:
        consumer.commit(
            offsets=[TopicPartition(RAW_TOPIC, PARTITION, bucket.last_offset + 1)],
            asynchronous=False,
        )
    except KafkaException as problem:
        print(f"[warn] {final} is written but its position was not recorded: {problem}")
    meter.files += 1


def archive(root: Path, minutes: int, verbose: bool) -> None:
    """Read the raw frames and write them to disk a bucket at a time, until stopped.

    Args:
        root: The archive root; files land under root/symbol=.../date=...
        minutes: Bucket length, and so how much a single file holds.
        verbose: True to print a line for every frame, rather than a progress line.
    """
    consumer = build_consumer()  # one consumer for the whole run, closed by the finally below
    meter = Meter()  # one set of counts for the whole run, printed every few seconds
    bucket: Bucket | None = None  # the bucket being gathered; None before the first frame
    closed_through: datetime | None = None  # newest bucket already on disk, never reopened
    arrival = datetime.now(UTC)  # stands in for a record that carries no usable timestamp
    running = True  # cleared by the handler below, which is the only thing that ends this run

    def on_signal(signum: int, frame: types.FrameType | None) -> None:
        """Treat a stop request from Docker and a Ctrl+C the same way.

        Args:
            signum: Which signal arrived.
            frame: The interrupted stack frame, which is not used.
        """
        nonlocal running
        print(f"\n[stopping] signal {signum}")
        running = False  # the fetch waits at most one timeout, then the loop below ends

    signal.signal(signal.SIGINT, on_signal)  # replaces the default that raises KeyboardInterrupt
    signal.signal(signal.SIGTERM, on_signal)  # without this Docker has to kill us outright
    root.mkdir(parents=True, exist_ok=True)  # the bind mount may hand us an empty host folder
    sweep_parts(root)  # anything half-written belongs to a run that is already gone
    start_position(consumer)  # refuses a reshaped topic, and says where this run picks up
    check_topic_rewound(consumer, root)  # says so if the topic was recreated under us
    # Subscribed as a group, which is what stops two archivers writing the same file: a second
    # one started by accident joins, is handed nothing, and writes nothing.
    consumer.subscribe([RAW_TOPIC])
    print(f"subscribed to {RAW_TOPIC} at {BROKERS} as group {GROUP_ID}")
    print(f"writing raw frames to {root} in {minutes} minute buckets")
    try:
        while running:
            for message in consumer.consume(BATCH_SIZE, POLL_TIMEOUT):
                error = message.error()
                if error is not None:  # a fetch problem must be visible, not swallowed
                    if error.code() != KafkaError._PARTITION_EOF:  # EOF is a marker, not a failure
                        print(f"[error] {error}")
                    continue
                kind, millis = message.timestamp()
                dated = kind != TIMESTAMP_NOT_AVAILABLE
                if dated:  # the broker's own clock, set by ingest when it produced the frame
                    arrival = datetime.fromtimestamp(millis / 1000, UTC)
                # An undated record keeps the previous arrival rather than jumping to our clock,
                # so it stays in the bucket its neighbours are in and the file cannot go backwards.
                wanted = bucket_start(arrival, minutes)
                if bucket is not None and wanted > bucket.start:  # this frame begins a new one
                    close_bucket(consumer, bucket, meter)
                    closed_through = bucket.start
                    bucket = None
                if bucket is None:
                    start = wanted
                    # Never open a bucket at a stretch of time already written. That file is on
                    # disk and its position recorded, so opening it again would write a second,
                    # shorter file straight over the first.
                    if closed_through is not None and wanted <= closed_through:
                        start = closed_through + timedelta(minutes=minutes)
                    bucket = Bucket(start, minutes, root, SYMBOL)
                # A record carrying no value at all still earns a line: the prefix keeps that
                # line non-empty, so the reserved empty line stays free and nothing raises here.
                bucket.add(message.offset(), arrival, message.value() or b"", dated)
                meter.read += 1
                if verbose:
                    print(
                        f"offset {message.offset()} at {stamp(arrival)} "
                        f"{len(message.value())} bytes -> {bucket.start:%H:%M}"
                    )
            # Outside the fetch, so a topic that has gone quiet still gets here. Rollover is
            # driven by the frames themselves, which is right -- no frames means nothing to
            # write -- but it leaves one hole: a producer that dies mid-bucket would strand that
            # bucket unwritten until it came back. So a bucket whose stretch of time ended a
            # while ago is closed anyway.
            #
            # Only once there is nothing left to read, though, and that condition is the whole
            # point rather than a refinement. Reading back a day of retained frames is the normal
            # way this service starts, and during that catch-up every bucket's stretch of time
            # ended hours ago by our clock -- so without the lag test this fires on every single
            # fetch, cutting each bucket short and writing a second short file over it.
            if (
                bucket is not None
                and lag_of(consumer) == 0  # None means not known yet, which is not caught up
                and datetime.now(UTC) - bucket.end > timedelta(seconds=IDLE_CLOSE_SECONDS)
            ):
                print(f"[idle] nothing published since {bucket.end:%H:%M}; closing that bucket")
                close_bucket(consumer, bucket, meter)
                closed_through = bucket.start
                bucket = None
            if meter.due():  # outside the fetch too, so a quiet topic still reports in
                print(meter.report(consumer, bucket))
    finally:
        # The bucket still being gathered is dropped and its position is not recorded. A file
        # named for a whole quarter hour that held twelve minutes would be indistinguishable
        # from a real one. Nothing is lost: those frames are still on the topic, and the next
        # run reads them again from the position last recorded.
        if bucket is not None:
            print(
                f"[dropped] the open bucket {bucket.start:%Y-%m-%dT%H:%M} held {bucket.rows} "
                f"frames, offsets {bucket.first_offset}-{bucket.last_offset}; "
                f"they stay on the topic"
            )
            bucket.discard()
        consumer.close()  # leave the group now rather than waiting out the session timeout
        elapsed = time.monotonic() - meter.started
        print(f"read {meter.read} frames in {elapsed:.1f}s from {RAW_TOPIC} at {BROKERS}")
        print(f"wrote {meter.files} files under {root}")


def main() -> None:
    """Parse the command line and run one archive."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=ARCHIVE_ROOT,
        help="where the archive lives; files land under ROOT/symbol=.../date=...",
    )
    parser.add_argument(
        "--bucket-minutes",
        type=int,
        default=BUCKET_MINUTES,
        help="minutes of frames in one file; must divide an hour evenly",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="print a line for every frame instead of a progress line every few seconds",
    )
    args = parser.parse_args()
    if not 1 <= args.bucket_minutes <= 60 or 60 % args.bucket_minutes:
        # bucket_start rounds the minute hand down, so anything that does not divide an hour
        # would make the last bucket of each hour a different length from the rest.
        parser.error(
            "--bucket-minutes must divide an hour evenly: 1, 2, 3, 5, 6, 10, 15, 20, 30 or 60"
        )
    archive(args.root, args.bucket_minutes, args.verbose)


if __name__ == "__main__":
    main()
