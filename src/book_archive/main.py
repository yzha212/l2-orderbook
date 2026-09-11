"""Write the published top-of-book records to Parquet files on disk.

This is stage 7 of the architecture. It reads the records the book builder publishes to
l2.book.btc-usd and gathers them into one file per quarter hour, so the book that only
lived for an hour on the topic can still be asked about in a year.

The topic is the write-ahead log. Offsets are committed only after a whole bucket has been
written, flushed to the disk and moved into place under its final name, which means a crash
rewinds to the last commit and writes that bucket again rather than losing it. That is also
why nothing here ever reopens a file it has closed: reopening would mean un-committing, and
the recovery story rests on never having to.

Parquet rather than the JSON these records arrive as, because this file exists to be
queried. Every record has the same shape -- ten levels a side, always the same columns --
so each column can be stored on its own and a question about the spread reads two columns
out of forty-five instead of parsing six hundred megabytes of text a day.

The file name says which quarter hour it holds, taken from the record's own published_at
rather than from this program's clock. That is what makes a replay land in the same file it
did the first time. It is a label for the layout and not a claim about the exchange's
clock; both clocks survive as columns, so a question asked in exchange time is a GROUP BY
on the timestamp column and gets the exact answer.
"""

import argparse
import os  # reads the broker list and the group name, and flushes files to the disk
import signal  # lets a stop request from Docker reach the loop below
import time
import types  # only for the stack frame a signal handler is handed
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import msgspec  # decodes the published record back into the struct the book builder wrote
import pyarrow as pa  # holds a bucket's rows as columns, which is what Parquet wants
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from src.book.book import TopLevels  # stage 4's published record; the contract this stage reads

BOOK_TOPIC = "l2.book.btc-usd"  # must match the topic `topic-init` creates in docker-compose.yml
PARTITION = 0  # the book topic is created with exactly one; start_position refuses any other count
BROKERS = os.environ.get("REDPANDA_BROKERS", "redpanda-0:9092")  # host runs use localhost:19092
GROUP_ID = os.environ.get("CONSUMER_GROUP", "l2-book-archive")  # compose sets it; default for host runs
ARCHIVE_ROOT = Path(os.environ.get("ARCHIVE_ROOT", "data"))  # /app/data in the container
SYMBOL = "BTC-USD"  # the one symbol this project follows, and the partition key on disk
DEPTH = 10  # levels a side, which fixes the column count and matches what stage 4 publishes
PRECISION = 18  # see build_schema: 18 or below is what lets a decimal be stored as an int64
SCALE = 8  # Coinbase quotes size to eight places and price to two, so eight covers both
BUCKET_MINUTES = 15  # one file per quarter hour
ROWS_PER_BATCH = 600  # a minute at ten records a second: how often Python objects become Arrow
ROWS_PER_GROUP = 3000  # five minutes a row group, so three blocks of statistics per file
BATCH_SIZE = 1000  # records asked for per fetch; this reader wants throughput, not freshness
POLL_TIMEOUT = 1.0  # seconds a fetch waits, which also paces the idle check and the progress line
IDLE_CLOSE_SECONDS = 60.0  # grace before a silent feed's last bucket is closed on our own clock
STATS_INTERVAL = 10.0  # seconds between progress lines
SCHEMA_VERSION = "1"  # bumped if the columns ever change, so a mixed read can tell them apart
WRITER = "l2-orderbook/book_archive"  # recorded in every file, so a file says what made it

_decoder = msgspec.json.Decoder(TopLevels)  # built once, as stage 3 and the dashboard do


def build_consumer() -> Consumer:
    """Create the consumer used to read the published book.

    Returns:
        A consumer configured to talk to the Redpanda broker.
    """
    return Consumer(  # config is a plain dict of librdkafka property names
        {
            "bootstrap.servers": BROKERS,  # where to find the cluster
            "group.id": GROUP_ID,  # a real group here: it holds the position and locks the writer
            "auto.offset.reset": "earliest",  # a group with no position starts at the oldest record
            # The rename is what earns a commit, never the clock. Auto-commit would move the
            # position on a timer while a bucket was still only in memory, so a crash would
            # skip past records that had never reached a file.
            "enable.auto.commit": False,
        }
    )


def build_schema(depth: int) -> pa.Schema:
    """Describe one row: where it came from, its two clocks, and a column per level.

    One column per price and per size rather than one list column holding all ten. The
    numbers are the same either way, but separate columns mean a reader after the best bid
    reads one column and skips the other forty-three, and the smallest and largest value
    Parquet records for each column then describes a single level instead of a whole side,
    which is what lets a search skip files without opening them. This is also the shape the
    market data vendors ship top-of-book snapshots in, so the files read like theirs do.

    decimal128 rather than a float, for the reason stage 4 gives for Decimal: a float cannot
    hold 0.00500000 without dropping the trailing zeros that say how finely this exchange
    quotes size. The precision of 18 is not taste either. Parquet may store a decimal of
    eighteen digits or fewer as a plain int64, which is half what a wider one costs and is
    the only kind of column the delta encoding in encodings_for will accept. Eight of those
    digits go to the fraction, leaving room for a price up to ten billion.

    Whether that combination was worth it was settled by measurement, not taste. One full
    fifteen minute bucket, 9,000 rows of ten levels a side, written three ways: as chosen
    here it came to 788,565 bytes, or 88 bytes a row; with the columns packed by dictionary
    instead it was 1,341,842; stored as wide blobs rather than int64 it was 975,306. The same
    rows as the JSON stage 4 puts on the wire are 6,176,696 bytes, so the file on disk is
    about an eighth of the feed that made it. Real buckets come out smaller still, because
    real sizes cluster on round numbers where those test rows were random to eight places:
    two full buckets off the live feed measured 100 and 64 bytes a row, which is somewhere
    around 50 to 80 MB a day and 20 to 30 GB a year. A quarter hour where the feed had
    stalled and the same book was republished all the way through came to 5 bytes a row.

    Args:
        depth: Levels a side, which is what decides how many columns there are.

    Returns:
        The schema for one bucket file, in column order: origin, clocks, bids, then asks.
    """
    level = pa.decimal128(PRECISION, SCALE)
    fields = [
        # The broker's position in the topic. Unique within this topic for as long as it
        # lives, which makes it both the row's identity and what a duplicate is found by,
        # and what a rewritten bucket is proved identical on. Named kafka_offset rather than
        # offset because offset is a reserved word in the query tools these files are read
        # with, and a bare one would need quoting in every query ever written against them.
        pa.field("kafka_offset", pa.int64(), nullable=False),
        pa.field("sequence_num", pa.int64()),  # null while stage 4 has applied no frame yet
        pa.field("timestamp", pa.timestamp("us", tz="UTC")),  # the exchange's clock, nullable
        pa.field("published_at", pa.timestamp("us", tz="UTC"), nullable=False),  # ours, always set
        pa.field("doubtful", pa.bool_(), nullable=False),  # stage 4 says the stream lost a frame
    ]
    for side in ("bid", "ask"):  # bid prices, bid sizes, then ask prices, ask sizes
        for kind in ("px", "sz"):
            # Two digits, so bid_px_09 sorts after bid_px_08 rather than before bid_px_1.
            fields += [pa.field(f"{side}_{kind}_{index:02d}", level) for index in range(depth)]
    return pa.schema(fields)


def encodings_for(schema: pa.Schema) -> dict[str, str]:
    """Choose how each column is packed, which is only worth deciding for the numbers.

    Every column here is a whole number underneath except one: the offsets, both clocks, and
    -- because the decimals are stored as int64 -- all forty price and size columns. Row to
    row those barely move, a tenth of a second or one tick apart, so storing the differences
    instead of the values leaves a handful of bits each for the compressor to work on.

    The doubtful column is left out deliberately rather than forgotten. It holds true or
    false, the difference encoding is for whole numbers only, and the list of settings this
    writer accepts has nothing suited to it, so naming it here would only raise. Left alone
    it is written plainly, and a column of nine thousand identical answers costs nothing once
    compressed anyway.

    Args:
        schema: The schema build_schema returned.

    Returns:
        Column name to encoding, for every column that is a whole number underneath.
    """
    return {name: "DELTA_BINARY_PACKED" for name in schema.names if name != "doubtful"}


def bucket_start(moment: datetime, minutes: int) -> datetime:
    """Round a moment down to the start of the bucket it falls in.

    Args:
        moment: Any instant; converted to UTC first, so the caller's zone cannot shift it.
        minutes: Bucket length, which main() has already checked divides an hour evenly.

    Returns:
        The first instant of that bucket, in UTC.
    """
    stamp = moment.astimezone(UTC)
    return stamp.replace(minute=stamp.minute - stamp.minute % minutes, second=0, microsecond=0)


def path_for(root: Path, symbol: str, start: datetime) -> Path:
    """Work out where one bucket's file belongs.

    The directories carry the meaning and the file name carries only what changes between
    files. symbol= and date= are not decoration: a query tool reads each name=value folder
    as a column, so asking for one day skips every other day's folder without opening a
    single file. Nothing reads file names that way, so repeating the symbol in the name
    would buy nothing. The date is repeated there, and only there, because someone will one
    day copy a pile of these into one folder and a name of just the time would let files
    from different days quietly overwrite each other.

    Args:
        root: The archive root, `data` unless the environment says otherwise.
        symbol: The product these records are for.
        start: The first instant of the bucket, from bucket_start.

    Returns:
        The full path of the finished file, which may not exist yet.
    """
    return (
        root
        / "book"  # a plain folder, not a name=value pair, so it stays out of the query columns
        / f"symbol={symbol}"
        / f"date={start:%Y-%m-%d}"
        / f"{start:%Y%m%dT%H%M}.parquet"
    )


def to_batch(rows: list[tuple[int, TopLevels]], schema: pa.Schema, depth: int) -> pa.RecordBatch:
    """Turn a minute's worth of decoded records into one block of columns.

    Called often rather than once at the end of the bucket, and that is the point. A quarter
    hour of these records held as Python objects is about three hundred and sixty thousand
    Decimals, which runs to tens of megabytes; the same rows held as columns are sixteen
    bytes a number, about six. Converting a minute at a time keeps the Python side of that
    bounded to a minute and leaves only the columns behind.

    A side that came up short simply has no level at that position, so the column takes an
    empty value rather than a zero there. Zero is a size someone might really have quoted;
    empty is the only answer that says nobody quoted anything at all.

    Args:
        rows: The offset and decoded record for each row, in the order they were read.
        schema: The schema build_schema returned, which fixes the column order.
        depth: Levels a side, matching that schema.

    Returns:
        One block holding those rows as columns.

    Raises:
        pyarrow.ArrowInvalid: If a price or size does not fit the decimal columns.
    """
    columns: list[pa.Array] = [
        pa.array([offset for offset, _ in rows], type=pa.int64()),
        pa.array([record.sequence_num for _, record in rows], type=pa.int64()),
        pa.array([record.timestamp for _, record in rows], type=schema.field("timestamp").type),
        pa.array(
            [record.published_at for _, record in rows],
            type=schema.field("published_at").type,
        ),
        pa.array([record.doubtful for _, record in rows], type=pa.bool_()),
    ]
    level = pa.decimal128(PRECISION, SCALE)
    for side in ("bid", "ask"):
        prices: list[list[Decimal | None]] = [[] for _ in range(depth)]
        sizes: list[list[Decimal | None]] = [[] for _ in range(depth)]
        for _, record in rows:  # one pass over the batch, filling all the columns for one side
            levels = record.bids if side == "bid" else record.asks
            for index in range(depth):
                try:  # EAFP: a thin side runs out here rather than being measured up front
                    price, size = levels[index]
                except IndexError:
                    price = size = None
                prices[index].append(price)
                sizes[index].append(size)
        columns += [pa.array(column, type=level) for column in prices]
        columns += [pa.array(column, type=level) for column in sizes]
    return pa.RecordBatch.from_arrays(columns, schema=schema)


class Bucket:
    """One quarter hour of records, gathered in memory until it is whole.

    Nothing reaches the disk until the bucket is closed, so a bucket that is dropped -- on a
    stop, or on a crash -- costs only the work of reading it again. The counters below are
    the quality signals that have until now died with the process: they go into the finished
    file so a reader years later can see what the stream was doing while it was recorded.

    Attributes:
        start: The first instant of this bucket, which decides the file name.
        end: The first instant of the next bucket, used by the idle check.
        rows: Rows accepted so far, across the blocks already converted and those pending.
        first_offset: Broker position of the first row, for the range this file covers.
        last_offset: Broker position of the most recent row, and what gets committed.
        doubtful_rows: Rows stage 4 marked as coming from a stream that lost a frame.
        crossed_rows: Rows where the best bid was at or above the best ask, which means the
            two sides disagreed.
        thin_rows: Rows where either side held fewer levels than asked for.
        late_rows: Rows published before this bucket began, kept here because their own
            bucket was already written and closed files are never reopened.
        dropped_rows: Rows refused by the decimal columns, counted rather than hidden.
        offset_gaps: Places where the broker position jumped, which on a single-partition
            topic means a record was skipped or had already expired.
    """

    def __init__(self, start: datetime, minutes: int, schema: pa.Schema, depth: int) -> None:
        """Open an empty bucket for one stretch of time.

        Args:
            start: The first instant of the bucket, from bucket_start.
            minutes: Bucket length, which decides when it is over.
            schema: The schema its rows are built against.
            depth: Levels a side, matching that schema.
        """
        self.start = start
        self.end = start + timedelta(minutes=minutes)
        self.schema = schema
        self.depth = depth
        self.blocks: list[pa.RecordBatch] = []  # rows already turned into columns
        self.pending: list[tuple[int, TopLevels]] = []  # rows still waiting to be converted
        self.rows = 0
        self.first_offset: int | None = None
        self.last_offset: int | None = None
        self.first_published_at: datetime | None = None
        self.last_published_at: datetime | None = None
        self.first_timestamp: datetime | None = None
        self.last_timestamp: datetime | None = None
        self.doubtful_rows = 0
        self.crossed_rows = 0
        self.thin_rows = 0
        self.late_rows = 0
        self.dropped_rows = 0
        self.offset_gaps = 0

    def add(self, offset: int, record: TopLevels, late: bool) -> None:
        """Take one record into the bucket and note what it says about the stream.

        Args:
            offset: The broker's position for this record.
            record: The decoded top of the book.
            late: True when this record was published before the bucket began.
        """
        if self.last_offset is not None and offset > self.last_offset + 1:
            self.offset_gaps += 1  # a hole in the topic, which the file should say out loud
        if self.first_offset is None:
            self.first_offset = offset
            self.first_published_at = record.published_at
            self.first_timestamp = record.timestamp
        self.last_offset = offset
        self.last_published_at = record.published_at
        if record.timestamp is not None:  # a null one would erase what the last real frame said
            self.last_timestamp = record.timestamp
        self.doubtful_rows += record.doubtful
        self.late_rows += late
        if len(record.bids) < self.depth or len(record.asks) < self.depth:
            self.thin_rows += 1
        try:  # EAFP: an empty side raises here rather than being measured first
            self.crossed_rows += record.bids[0][0] >= record.asks[0][0]
        except IndexError:
            pass  # one empty side cannot cross the other, exactly as stage 4 decides it
        self.pending.append((offset, record))
        self.rows += 1

    def flush(self) -> None:
        """Turn the rows waiting in Python into one block of columns.

        A price too large or a size quoted more finely than the columns allow makes the whole
        block refuse to convert. Rather than lose the minute, the block is rebuilt a row at a
        time so that only the offending row is dropped, and its offset and value are printed
        and counted. That is the same posture stage 4 takes towards a negative size: named
        and counted, never silently swallowed, and never a reason to stop the run.
        """
        if not self.pending:
            return
        try:
            self.blocks.append(to_batch(self.pending, self.schema, self.depth))
        except pa.ArrowInvalid as problem:
            print(f"[warn] a row in this block does not fit the columns: {problem}")
            kept: list[tuple[int, TopLevels]] = []
            for offset, record in self.pending:
                try:
                    to_batch([(offset, record)], self.schema, self.depth)
                except pa.ArrowInvalid as row_problem:
                    print(f"[dropped] offset {offset}: {row_problem}")
                    self.dropped_rows += 1
                    self.rows -= 1
                    continue
                kept.append((offset, record))
            if kept:
                self.blocks.append(to_batch(kept, self.schema, self.depth))
        self.pending.clear()

    def table(self, manifest: dict[str, str]) -> pa.Table:
        """Gather every block into one table carrying this bucket's manifest.

        The manifest rides in the file's own metadata rather than in a file beside it,
        because a second file can go missing or drift out of step with the one it describes
        and then there are two answers to the same question.

        Args:
            manifest: What manifest_of returned for this bucket.

        Returns:
            Every row of the bucket, in the order they were read.
        """
        return pa.Table.from_batches(self.blocks, schema=self.schema).replace_schema_metadata(
            manifest
        )


def manifest_of(bucket: Bucket, minutes: int, depth: int) -> dict[str, str]:
    """Describe a finished bucket, for the metadata the file carries.

    Only what the file cannot work out for itself. The row count and the largest and smallest
    value of every column are already in the file; what is not is where the rows came from
    and what the stream was doing while they were recorded. The keys are flat strings rather
    than one packed blob, because a query tool hands this metadata back as plain rows and
    flat keys can be read there without unpacking anything first.

    Args:
        bucket: The bucket about to be written.
        minutes: Bucket length, so a reader knows what a whole one should hold.
        depth: Levels a side, so a reader can tell one generation of files from another.

    Returns:
        Metadata keys to values, all of them text, which is what the file format holds.
    """
    values = {
        "schema_version": SCHEMA_VERSION,
        "symbol": SYMBOL,
        "depth": depth,
        "bucket_start": f"{bucket.start:%Y-%m-%dT%H:%M:%SZ}",
        "bucket_minutes": minutes,
        "source_topic": BOOK_TOPIC,
        "source_partition": PARTITION,
        "consumer_group": GROUP_ID,
        "rows": bucket.rows,
        "first_offset": bucket.first_offset,
        "last_offset": bucket.last_offset,
        "first_published_at": bucket.first_published_at,
        "last_published_at": bucket.last_published_at,
        "first_timestamp": bucket.first_timestamp,
        "last_timestamp": bucket.last_timestamp,
        "doubtful_rows": bucket.doubtful_rows,
        "crossed_rows": bucket.crossed_rows,
        "thin_rows": bucket.thin_rows,
        "late_rows": bucket.late_rows,
        "dropped_rows": bucket.dropped_rows,
        "offset_gaps": bucket.offset_gaps,
        "written_at": datetime.now(UTC),
        "writer": WRITER,
    }
    # Prefixed so these never collide with anything the writer itself records in there.
    return {f"l2.{key}": str(value) for key, value in values.items()}


def write_bucket(table: pa.Table, part: Path, final: Path) -> None:
    """Write one bucket's rows, then make them visible in a single step.

    Written under a name nothing looks for, pushed all the way to the disk, and only then
    moved into place. The move is one operation, so a reader either sees the whole bucket or
    does not see the file at all; there is no moment where a half-written bucket wears the
    name of a finished one. The folder is flushed too, because a move that is still only in
    memory does not survive losing power.

    Replacing a file that already exists is expected rather than a fault. It means the last
    run wrote this bucket and was stopped before it could record its position, so this run
    read the same records again and built the same rows from them.

    Args:
        table: Every row of the bucket, in the order they were read.
        part: The name to write under, which nothing reads.
        final: The name to move it to once it is whole.
    """
    final.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        table,
        part,
        compression="zstd",  # the columns are near-repeats row to row, which zstd eats
        compression_level=3,  # the usual setting; higher buys single digits here and costs time
        use_dictionary=False,  # required: the encodings below are refused while this is on
        column_encoding=encodings_for(table.schema),
        store_decimal_as_integer=True,  # what makes a decimal an int64 rather than a wide blob
        write_statistics=True,  # the largest and smallest per column, which lets a search skip
        row_group_size=ROWS_PER_GROUP,
        write_page_index=True,  # not the default; lets a reader skip within a block as well
        version="2.6",  # already the default, said out loud because the encodings need it
        sorting_columns=pq.SortingColumn.from_ordering(
            table.schema, [("kafka_offset", "ascending")]
        ),
    )
    with part.open("rb") as handle:  # opened only to push what was just written to the disk
        os.fsync(handle.fileno())
    part.replace(final)  # one operation, and it overwrites a rewritten bucket without complaint
    folder = os.open(final.parent, os.O_RDONLY)  # the move itself has to reach the disk too
    try:
        os.fsync(folder)
    finally:
        os.close(folder)


def sweep_parts(root: Path) -> None:
    """Delete any half-written file a killed run left behind.

    These are never read and are rubbish by definition: the run that was writing one never
    got as far as moving it into place, so whatever it holds was rewritten from the topic.

    Args:
        root: The archive root to sweep.
    """
    for leftover in sorted(root.glob("book/**/*.parquet.part")):
        print(f"[swept] {leftover} was left half-written by an earlier run")
        leftover.unlink()


def start_position(consumer: Consumer) -> None:
    """Check the topic is shaped the way this writer assumes, and say where it resumes.

    Two things are worth knowing before a single record is read. The first is that the topic
    still has exactly one partition: this writer treats the broker position as a row's
    identity and as the range a file covers, and both of those stop being true the moment two
    independent positions are interleaved into one file. The second is whether the position
    this group recorded still exists. If it has fallen off the start of the topic, records
    were deleted before they were ever written to a file, and that is the one loss this
    design cannot undo -- so it is printed loudly rather than resumed over in silence.

    Args:
        consumer: The consumer, already built but not yet reading.

    Raises:
        KafkaException: If the topic is missing, or no longer has one partition.
    """
    topic = consumer.list_topics(BOOK_TOPIC, timeout=10.0).topics[BOOK_TOPIC]
    if topic.error is not None:
        raise KafkaException(topic.error)
    if len(topic.partitions) != 1:  # see the docstring: the whole file layout rests on this
        raise KafkaException(
            f"{BOOK_TOPIC} has {len(topic.partitions)} partitions; this writer needs exactly one"
        )
    spot = TopicPartition(BOOK_TOPIC, PARTITION)
    committed = consumer.committed([spot], timeout=10.0)[0].offset
    low, high = consumer.get_watermark_offsets(spot, timeout=10.0, cached=False)
    if committed < 0:  # no position recorded yet, so auto.offset.reset decides where to begin
        print(f"[position] no position recorded for {GROUP_ID}; starting at {low}")
        return
    if committed < low:  # the one case where records are gone for good
        print(
            f"[warn] {GROUP_ID} was at {committed} but the topic now starts at {low}: "
            f"{low - committed} records expired before they were archived"
        )
        return
    print(f"[position] resuming {GROUP_ID} at {committed}, topic holds {low} to {high}")


class Meter:
    """Counts for the progress line, so a long run says what it is doing.

    Attributes:
        read: Records decoded since the run began.
        written: Rows written to finished files.
        files: Finished files written.
        started: When the run began, for the closing summary.
    """

    def __init__(self) -> None:
        """Start every count at zero, from now."""
        self.read = 0
        self.written = 0
        self.files = 0
        self.started = time.monotonic()
        self.at = self.started + STATS_INTERVAL  # when the next line is owed
        self.timed = False  # whether a line has been printed at all, which the summary checks

    def due(self) -> bool:
        """Say whether a progress line is owed, and book the next one if so.

        Returns:
            True when one interval has passed since the last line.
        """
        now = time.monotonic()
        if now < self.at:
            return False
        self.at = now + STATS_INTERVAL
        self.timed = True
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
            f"lag {'?' if lag is None else lag} "
            f"files {self.files} rows {self.written} bucket {open_now}"
        )


def lag_of(consumer: Consumer) -> int | None:
    """Say how many records are still to read before the end of the topic.

    Uses the watermarks the client already has from its last fetch, so this costs no call to
    the broker and can sit both on a line printed every few seconds and in the idle check
    that runs after every fetch.

    Args:
        consumer: The consumer to ask.

    Returns:
        Records still to read, or None while there is no position or no watermark yet --
        which is not the same as zero, and the caller has to tell the two apart.
    """
    try:  # EAFP: before the first fetch there is no position and no watermark to compare
        spot = consumer.position([TopicPartition(BOOK_TOPIC, PARTITION)])[0]
        _, high = consumer.get_watermark_offsets(spot, cached=True)
        if spot.offset < 0 or high < 0:  # the client's stand-in for "not known yet"
            return None
        return max(high - spot.offset, 0)
    except (KafkaException, TypeError):
        return None


def close_bucket(consumer: Consumer, bucket: Bucket, root: Path, minutes: int, meter: Meter) -> None:
    """Write a finished bucket to disk and only then record the position it reached.

    The order is the whole durability story. If this run dies between the write and the
    commit, the next one reads the same records again and writes the same file over the top;
    if it died the other way round, those records would be skipped for good.

    Args:
        consumer: The consumer whose position is recorded once the file is safe.
        bucket: The finished bucket.
        root: The archive root.
        minutes: Bucket length, recorded in the file's metadata.
        meter: Counts to update once the file has landed.
    """
    bucket.flush()
    if not bucket.blocks:  # every row was refused by the columns, so there is nothing to write
        print(f"[empty] {bucket.start:%Y-%m-%dT%H:%M} held no rows that fit; nothing written")
        return
    final = path_for(root, SYMBOL, bucket.start)
    part = final.with_name(f"{final.name}.part")
    rewrite = final.exists()  # expected after a crash; see write_bucket
    write_bucket(bucket.table(manifest_of(bucket, minutes, bucket.depth)), part, final)
    faults = (
        f"doubtful {bucket.doubtful_rows}, crossed {bucket.crossed_rows}, "
        f"thin {bucket.thin_rows}, late {bucket.late_rows}, "
        f"dropped {bucket.dropped_rows}, gaps {bucket.offset_gaps}"
    )
    kind = "rewrote" if rewrite else "closed"
    print(
        f"[{kind}] {final} rows {bucket.rows} "
        f"offsets {bucket.first_offset}-{bucket.last_offset} ({faults})"
    )
    # Committed last, and synchronously, so a refusal is visible here rather than discovered
    # as a rewritten bucket much later. A refusal is not fatal: the file is already on disk
    # and the next run simply writes this bucket again.
    try:
        consumer.commit(
            offsets=[TopicPartition(BOOK_TOPIC, PARTITION, bucket.last_offset + 1)],
            asynchronous=False,
        )
    except KafkaException as problem:
        print(f"[warn] {final} is written but its position was not recorded: {problem}")
    meter.files += 1
    meter.written += bucket.rows


def archive(root: Path, depth: int, minutes: int, verbose: bool) -> None:
    """Read the published book and write it to disk a bucket at a time, until stopped.

    Args:
        root: The archive root; files land under root/book.
        depth: How many levels a side to keep, which must match what stage 4 publishes.
        minutes: Bucket length, and so how much a single file holds.
        verbose: True to print a line for every record, rather than a progress line.
    """
    schema = build_schema(depth)
    consumer = build_consumer()  # one consumer for the whole run, closed by the finally below
    meter = Meter()  # one set of counts for the whole run, printed every few seconds
    bucket: Bucket | None = None  # the bucket being gathered; None before the first record
    closed_through: datetime | None = None  # newest bucket already on disk, never reopened
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
    sweep_parts(root)  # anything half-written belongs to a run that is already gone
    start_position(consumer)  # refuses a reshaped topic, and says where this run picks up
    # Subscribed as a group, which is the one place this reader parts from the dashboard and
    # for the opposite reason. The dashboard takes the partition outright so that any number
    # of people can watch at once; this writer needs exactly one of itself per file, and the
    # group is what enforces that. A second archiver started by accident joins, is handed
    # nothing, and writes nothing -- which is what you want -- rather than racing the first
    # one for the same file name.
    consumer.subscribe([BOOK_TOPIC])
    print(f"subscribed to {BOOK_TOPIC} at {BROKERS} as group {GROUP_ID}")
    print(f"writing top {depth} to {root / 'book'} in {minutes} minute buckets")
    try:
        while running:
            for message in consumer.consume(BATCH_SIZE, POLL_TIMEOUT):
                error = message.error()
                if error is not None:  # a fetch problem must be visible, not swallowed
                    if error.code() != KafkaError._PARTITION_EOF:  # EOF is a marker, not a failure
                        print(f"[error] {error}")
                    continue
                try:  # EAFP: a record that will not decode is named and skipped, never guessed at
                    record = _decoder.decode(message.value())
                except (msgspec.ValidationError, msgspec.DecodeError) as problem:
                    print(f"[unparsed] offset {message.offset()}: {problem}")
                    continue
                wanted = bucket_start(record.published_at, minutes)
                if bucket is not None and wanted > bucket.start:  # this record begins a new one
                    close_bucket(consumer, bucket, root, minutes, meter)
                    closed_through = bucket.start
                    bucket = None
                if bucket is None:
                    start = wanted
                    # Never open a bucket at a stretch of time already written. That file is
                    # on disk and its position recorded, so opening it again would write a
                    # second, shorter file straight over the first and lose every row the
                    # first one held. A record that belongs there instead joins the next
                    # bucket along and is counted as late below.
                    if closed_through is not None and wanted <= closed_through:
                        start = closed_through + timedelta(minutes=minutes)
                    bucket = Bucket(start, minutes, schema, depth)
                # A record that did not land in its own bucket is counted as late. It can only
                # come from a clock that stepped backwards or from the idle close below, and
                # the file it belongs to is not reopened for it, because reopening would mean
                # taking back a position already recorded and everything here rests on never
                # doing that. Nothing is lost: the row still carries the moment it was
                # published, so a question asked in published time gets the right answer
                # whichever file the row happens to sit in.
                bucket.add(message.offset(), record, wanted != bucket.start)
                if len(bucket.pending) >= ROWS_PER_BATCH:  # keep the Python side of memory small
                    bucket.flush()
                meter.read += 1
                if verbose:
                    print(
                        f"offset {message.offset()} seq={record.sequence_num} "
                        f"at {record.published_at:%H:%M:%S.%f} -> {bucket.start:%H:%M}"
                    )
            # Outside the fetch, so a topic that has gone quiet still gets here. Rollover is
            # driven by the records themselves, which is right -- no records means nothing to
            # write -- but it leaves one hole: a producer that dies mid-bucket would strand
            # that bucket unwritten until it came back, however long that took. So a bucket
            # whose stretch of time ended a while ago is closed anyway.
            #
            # Only once there is nothing left to read, though, and that condition is the whole
            # point rather than a refinement. Reading back a day of retained records is the
            # normal way this service starts, and during that catch-up every bucket's stretch
            # of time ended hours ago by our clock -- so without the lag test this fires on
            # every single fetch, cutting each bucket short and writing a second short file
            # over it. Measured on a replay of 36,061 records it lost 10,875 of them, a little
            # under a third, before anything noticed. Waiting for the end of the topic is what
            # makes "the feed has gone quiet" mean that and not "these records are old".
            if (
                bucket is not None
                and lag_of(consumer) == 0  # None means not known yet, which is not caught up
                and datetime.now(UTC) - bucket.end > timedelta(seconds=IDLE_CLOSE_SECONDS)
            ):
                print(f"[idle] nothing published since {bucket.end:%H:%M}; closing that bucket")
                close_bucket(consumer, bucket, root, minutes, meter)
                closed_through = bucket.start
                bucket = None
            if meter.due():  # outside the fetch too, so a quiet topic still reports in
                print(meter.report(consumer, bucket))
    finally:
        # The bucket still being gathered is dropped, and its position is not recorded. A file
        # named for a whole quarter hour that held twelve minutes would be indistinguishable
        # from a real one, and committing it instead would force a second file for the same
        # stretch of time, which breaks both the one-file-per-bucket layout and the offset
        # range that describes it. Nothing is lost: those records are still on the topic, and
        # the next run reads them again from the position last recorded.
        if bucket is not None:
            print(
                f"[dropped] the open bucket {bucket.start:%Y-%m-%dT%H:%M} held {bucket.rows} rows, "
                f"offsets {bucket.first_offset}-{bucket.last_offset}; they stay on the topic"
            )
        consumer.close()  # leave the group now rather than waiting out the session timeout
        elapsed = time.monotonic() - meter.started
        print(f"read {meter.read} records in {elapsed:.1f}s from {BOOK_TOPIC} at {BROKERS}")
        print(f"wrote {meter.written} rows in {meter.files} files under {root / 'book'}")


def main() -> None:
    """Parse the command line and run one archive."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=ARCHIVE_ROOT,
        help="where the archive lives; files land under ROOT/book",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=DEPTH,
        help="price levels to keep from each side of the book",
    )
    parser.add_argument(
        "--bucket-minutes",
        type=int,
        default=BUCKET_MINUTES,
        help="minutes of records in one file; must divide an hour evenly",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="print a line for every record instead of a progress line every few seconds",
    )
    args = parser.parse_args()
    if args.depth < 1:  # a depth of zero would give every row a side with no columns at all
        parser.error("--depth must be 1 or more")
    if not 1 <= args.bucket_minutes <= 60 or 60 % args.bucket_minutes:
        # bucket_start rounds the minute hand down, so anything that does not divide an hour
        # would make the last bucket of each hour a different length from the rest.
        parser.error("--bucket-minutes must divide an hour evenly: 1, 2, 3, 5, 6, 10, 15, 20, 30 or 60")
    archive(args.root, args.depth, args.bucket_minutes, args.verbose)


if __name__ == "__main__":
    main()
