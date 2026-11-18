"""Read the raw Coinbase Level 2 frames back off the Redpanda topic and build the book.

Frames are turned into typed objects by src.decode.messages and applied to the in-memory
book in src.book.book. The reader runs until it is stopped: an empty topic is something to
wait on, not a reason to finish, and nothing is written to disk.

Every frame is also checked against the exchange's own sequence number, so a frame that
never reached us is named on the line where it went missing rather than passed on unnoticed;
a gap marks the book doubtful, because from then on it may have drifted from the exchange.

Instead of a line per frame there is a progress line every few seconds, carrying how far
behind the topic the reader is and how long a frame costs to handle. Those two numbers are
what say whether the book is keeping up. Pass --verbose for the old line-per-frame output.

The top of that book is written to a topic of its own on a fixed rhythm, ten times a second
by default, which is what the dashboard and the book archiver will read. Use --depth and
--interval-ms to change how many levels each record carries and how often one goes out.
"""

import argparse
import os  # reads the broker list from the environment
import signal  # lets a stop request from Docker reach the code below
import time
import types  # only for the stack frame a signal handler is handed

import msgspec  # for the decode errors stage 3 raises, and to encode what we publish
from confluent_kafka import (  # librdkafka client
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    Producer,
)

from src.book.book import OrderBook  # stage 4: the in-memory book these frames build
from src.decode.messages import (  # stage 3: the one place raw JSON becomes typed objects
    ErrorFrame,
    Frame,
    HeartbeatFrame,
    L2Frame,
    SubscriptionsFrame,
    UnknownFrame,
    channel_of,
    decode_frame,
    event_type,
)

RAW_TOPIC = "l2.raw.btc-usd"  # must match the topic `topic-init` creates in docker-compose.yml
BOOK_TOPIC = "l2.book.btc-usd"  # the topic this service writes the top of the book to
BROKERS = os.environ.get("REDPANDA_BROKERS", "redpanda-0:9092")  # host runs use localhost:19092
GROUP_ID = os.environ.get("CONSUMER_GROUP", "l2-book")  # compose sets it; default for host runs
MAX_MESSAGE_BYTES = 20 * 1024 * 1024  # the opening snapshot runs ~5 MB, over the 1 MB default
BATCH_SIZE = 100  # records asked for per fetch, so catching up costs fewer round trips
STATS_INTERVAL = 5.0  # seconds between progress lines
PAYLOAD_PREVIEW = 120  # characters of a malformed frame to show, so the log stays readable
DEPTH = 10  # price levels published from each side of the book
INTERVAL_MS = 100.0  # milliseconds between published records
FLUSH_TIMEOUT = 5.0  # seconds spent draining the producer queue before the process exits

_encoder = msgspec.json.Encoder()  # built once, as stage 3 does with its decoders


def build_consumer() -> Consumer:
    """Create the consumer used to read raw frames back.

    The group never commits: with `enable.auto.commit` off and no manual commit, the group
    never acquires a stored position, so `auto.offset.reset` applies on every run and each
    run reads the topic from the beginning. That is deliberate for now. Coinbase only sends
    a book snapshot when ingest subscribes, so a reader that resumed from a stored position
    would start with an empty book and skip every update until ingest happened to reconnect.
    Reading from the beginning always finds a snapshot, at the cost of a replay on restart.

    The fetch size limit is raised for the same reason the producer's send limit is: the first
    frame of the feed is a full book snapshot, thousands of times larger than the updates after
    it. The broker returns an oversized first batch regardless, to guarantee progress, but
    matching the producer here keeps a 5 MB snapshot from costing a fetch round trip of its own.

    Returns:
        A consumer configured to talk to the Redpanda broker.
    """
    return Consumer(  # config is a plain dict of librdkafka property names
        {
            "bootstrap.servers": BROKERS,  # where to find the cluster
            "group.id": GROUP_ID,  # identifies the group whose offsets we would commit
            "auto.offset.reset": "earliest",  # a group with no position starts at the oldest record
            "enable.auto.commit": False,  # never commit, so every run replays the topic
            "fetch.message.max.bytes": MAX_MESSAGE_BYTES,  # must clear the snapshot frame's size
        }
    )


def build_producer() -> Producer:
    """Create the producer used to publish the top of the book.

    Two settings differ from the producer in src/ingest/main.py, both on purpose. `linger.ms`
    is zero rather than five: at ten records a second there is nothing to gather into a batch,
    and the whole point of this feed is being current. The send size limit is left at its
    default, because a record carrying ten levels a side is about a kilobyte, nowhere near
    the megabyte that default allows.

    Returns:
        A producer configured to talk to the Redpanda broker.
    """
    return Producer(  # config is a plain dict of librdkafka property names
        {
            "bootstrap.servers": BROKERS,  # where to find the cluster
            "linger.ms": 0,  # send as soon as there is anything to send
            "compression.type": "lz4",  # consecutive records are nearly identical JSON
            "acks": "all",  # only count a record delivered once the broker has it durably
            "enable.idempotence": True,  # a retry cannot reorder or duplicate records
        }
    )


def on_delivery(error: KafkaError | None, message: Message) -> None:
    """Report a record that never landed; a delivered one is counted, not printed.

    Args:
        error: The broker-side failure, or None when the record landed.
        message: The delivered record, carrying its partition and offset.
    """
    if error is not None:  # a record that never landed must be visible, not swallowed
        print(f"[delivery failed] {error}")


class SequenceWatch:
    """Track the exchange's frame counter so a missed frame is named rather than passed on.

    Every frame one connection carries -- level2, heartbeats, subscription acknowledgements
    alike -- has a sequence_num that goes up by exactly one. The heartbeats subscription is
    what makes that worth checking: a heartbeat lands about once a second even when the book
    is quiet, so the counter keeps moving and a frame lost during a lull still shows as a jump.
    """

    def __init__(self) -> None:
        """Start with no history, which is what lets the first frame set the baseline."""
        self.last: int | None = None  # the previous frame's counter, None until the first one
        self.checked = 0  # frames that carried a counter at all, which is what a gap is out of
        self.gaps = 0  # times the counter jumped, each jump one run of frames we never saw
        self.missed = 0  # how many frames those jumps add up to

    def check(self, frame: Frame) -> str:
        """Record what one frame does to the sequence.

        A counter lower than the one before it is passed over in silence: that is either a
        frame that arrived out of order, which the exchange says can be ignored, or the
        exchange starting a fresh count after ingest reconnected. Either way the new value
        becomes the baseline, so the frames after it stay checkable.

        Args:
            frame: The typed frame decode_frame returned.

        Returns:
            A note to append to the summary line, starting with a space, or an empty string
            when the counter did what it should.
        """
        try:  # EAFP: an exchange error frame is the only kind that carries no counter at all
            sequence = frame.sequence_num
        except AttributeError:
            return ""
        if sequence is None:  # an unmodelled channel whose frame carried no counter either
            return ""
        previous = self.last  # read before the update below, because the test needs both
        self.last = sequence
        self.checked += 1
        if previous is None or sequence <= previous + 1:  # the first frame, or an unbroken run
            return ""
        missed = sequence - previous - 1  # every counter the exchange used that we never saw
        self.gaps += 1
        self.missed += missed
        return f" [gap seq {previous}->{sequence} missed {missed}]"

    def report(self) -> str:
        """Summarise what the whole run found.

        Returns:
            One line naming how much was checked and what broke.
        """
        return f"[stream] checked {self.checked} frames, {self.gaps} gaps ({self.missed} missed)"


class Meter:
    """Count what the reader is doing and turn it into one progress line every few seconds.

    The timings are the point of this class. `decode` is what it costs to turn one record's
    JSON into a typed frame, and `apply` what it costs to put that frame into the book; a
    figure that climbs as the book fills would mean the book itself is the bottleneck. `lag`
    is the plainer test: as long as it stays near zero the reader is keeping up.

    Attributes:
        read: Records handled since the run started.
        started: When the run began, for the closing total.
    """

    def __init__(self) -> None:
        """Start with nothing counted and both clocks running."""
        self.read = 0
        self.started = time.monotonic()
        self.last = self.started  # when the previous progress line went out
        self.timed = 0  # records measured this window, which the averages divide by
        self.decode_ns = 0  # time spent turning JSON into typed frames this window
        self.apply_ns = 0  # time spent applying those frames to the book this window
        self.worst_ns = 0  # the slowest single record this window, usually a snapshot

    def record(self, decode_ns: int, apply_ns: int) -> None:
        """Add one record's timings to the current window.

        Args:
            decode_ns: Nanoseconds the decode took.
            apply_ns: Nanoseconds applying it to the book took.
        """
        self.read += 1
        self.timed += 1
        self.decode_ns += decode_ns
        self.apply_ns += apply_ns
        self.worst_ns = max(self.worst_ns, decode_ns + apply_ns)

    def due(self) -> bool:
        """Say whether enough time has passed to print another progress line.

        Returns:
            True once STATS_INTERVAL seconds have gone by since the last one.
        """
        return time.monotonic() - self.last >= STATS_INTERVAL

    def report(self, consumer: Consumer, book: OrderBook) -> str:
        """Build one progress line and start a fresh window.

        Args:
            consumer: The consumer, asked how far behind the end of the topic it is.
            book: The book, asked how many price levels it now holds.

        Returns:
            The line to print.
        """
        now = time.monotonic()
        window = now - self.last
        rate = self.timed / window if window else 0.0  # frames a second over this window only
        decode_ms = self.decode_ns / self.timed / 1e6 if self.timed else 0.0
        apply_ms = self.apply_ns / self.timed / 1e6 if self.timed else 0.0
        line = (
            f"[stats] read {self.read} ({rate:.0f}/s) {lag_of(consumer)} "
            f"decode {decode_ms:.2f}ms apply {apply_ms:.2f}ms max {self.worst_ns / 1e6:.0f}ms "
            f"bids {len(book.bids)} asks {len(book.asks)}"
        )
        self.last = now  # everything below resets, so each line describes its own window
        self.timed = self.decode_ns = self.apply_ns = self.worst_ns = 0
        return line


class Publisher:
    """Write the top of the book to its own topic on a fixed rhythm.

    The rhythm is kept by a due time rather than a sleep, because this shares a thread with
    the reader: the fetch in the loop below is told to wait only as long as there is time
    before the next record, so even a silent topic wakes up in time to publish. A frame that
    takes longer than one interval to apply -- the opening snapshot does -- pushes a record
    out late instead of blocking, and `late` in the progress line says by how much.

    Attributes:
        depth: How many levels each record carries from each side of the book.
        published: Records written since the run started.
    """

    def __init__(self, producer: Producer, depth: int, interval_ms: float) -> None:
        """Start the rhythm one interval from now.

        Args:
            producer: The producer to write records with.
            depth: How many levels to take from each side of the book.
            interval_ms: Milliseconds between records, in the unit the command line uses.
        """
        self.producer = producer
        self.depth = depth  # fixed for the run, so it is held here rather than passed per record
        self.interval = interval_ms / 1000  # the loop works in seconds, the flag in milliseconds
        self.due_at = time.monotonic() + self.interval  # when the next record is owed
        self.published = 0
        self.dropped = 0  # records the producer refused, which must not pass in silence
        self.window = 0  # records written this window, which the rate below divides
        self.last = time.monotonic()  # when the previous progress line went out
        self.late_ns = 0  # the worst lateness this window, so a delayed record is visible

    def seconds_left(self) -> float:
        """Say how long there is until the next record is owed.

        Returns:
            The wait in seconds, never below zero.
        """
        return max(0.0, self.due_at - time.monotonic())

    def publish_if_due(self, book: OrderBook) -> None:
        """Write one record if the interval has elapsed, and nothing at all if it has not.

        Args:
            book: The book to take the top levels from. Nothing is written until a snapshot
                has been loaded, because an empty book has no top to describe.
        """
        now = time.monotonic()
        if now < self.due_at:  # much the commoner case, so it costs one comparison
            return
        self.late_ns = max(self.late_ns, int((now - self.due_at) * 1e9))
        self.due_at += self.interval  # one whole interval on, so the rhythm never drifts
        if self.due_at < now:  # a slow frame left whole intervals behind us; do not replay them
            self.due_at = now + self.interval
        if not book.loaded:  # no snapshot yet, so there is nothing worth publishing
            return
        payload = _encoder.encode(book.top(self.depth))  # the depth reaches the book here
        try:  # EAFP: hand it over, and only deal with a refusal if one actually happens
            self.producer.produce(BOOK_TOPIC, value=payload, on_delivery=on_delivery)
        except (BufferError, KafkaException) as error:  # a full queue, or a rejected record
            self.dropped += 1
            print(f"[warn] top of book not published: {error}")
            return
        self.published += 1
        self.window += 1
        self.producer.poll(0)  # serves the delivery callbacks queued since the last record

    def report(self) -> str:
        """Build the publishing half of one progress line and start a fresh window.

        Returns:
            The part of the line that says what has been published.
        """
        now = time.monotonic()
        window = now - self.last
        rate = self.window / window if window else 0.0  # records a second over this window only
        line = (
            f"published {self.published} ({rate:.0f}/s) "
            f"late {self.late_ns / 1e6:.0f}ms dropped {self.dropped}"
        )
        self.last = now  # both counters below reset, so each line describes its own window
        self.window = self.late_ns = 0
        return line

    def close(self) -> int:
        """Drain whatever is still queued, so a stop does not lose the last records.

        Returns:
            How many records were still undelivered when the wait ran out.
        """
        return self.producer.flush(FLUSH_TIMEOUT)  # the one deliberate block, as ingest has


def lag_of(consumer: Consumer) -> str:
    """Say how far behind the end of the topic the consumer currently is.

    The high watermark is taken from the last fetch response rather than asked for over the
    network, so calling this every few seconds costs nothing.

    Args:
        consumer: The consumer to ask.

    Returns:
        The lag and the offset of the last record handled, or dashes while the group is
        still joining and has no partition to measure.
    """
    try:  # EAFP: before the group finishes joining there is nothing assigned to measure
        partition = consumer.assignment()[0]
        position = consumer.position([partition])[0].offset  # the offset we would read next
        _, high = consumer.get_watermark_offsets(partition, cached=True)
    except (IndexError, KafkaException):
        return "lag - offset -"
    if position < 0 or high < 0:  # a position the client has not established yet reads negative
        return "lag - offset -"
    return f"lag {high - position} offset {position - 1}"


def handle(
    message: Message, watch: SequenceWatch, book: OrderBook, meter: Meter
) -> tuple[Frame | None, str]:
    """Decode one record, apply it to the book, and time both halves.

    Args:
        message: The record fetched from the topic.
        watch: The tracker, updated with whatever counter this frame turned out to carry.
        book: The book, updated when the frame turns out to carry level2 events.
        meter: The counter, given how long this record took.

    Returns:
        The typed frame and the sequence note to go with it, or None and an empty note when
        the payload could not be decoded at all.
    """
    started = time.perf_counter_ns()
    try:  # EAFP: decode, and only deal with the frames stage 3 refuses if there are any
        frame = decode_frame(message.value())
    except msgspec.ValidationError as error:  # listed first, because it subclasses DecodeError
        print(f"offset={message.offset()} [invalid] {error}")  # refused, so it never reaches a book
        return None, ""
    except msgspec.DecodeError:  # not JSON at all, so there is nothing to report but bytes
        preview = message.value()[:PAYLOAD_PREVIEW].decode("utf-8", errors="replace")
        print(f"offset={message.offset()} [unparsed] {preview}")
        return None, ""
    decoded = time.perf_counter_ns()  # split here, so decode and book work are told apart
    note = watch.check(frame)  # empty unless this frame's counter skipped ahead
    if note:  # a frame we never saw may have moved a level the book still shows
        book.doubtful = True
    if isinstance(frame, L2Frame):  # the only channel carrying levels; the rest inform nothing
        book.apply(frame)
    meter.record(decoded - started, time.perf_counter_ns() - decoded)
    return frame, note


def describe(offset: int, frame: Frame, note: str, book: OrderBook) -> str:
    """Build the one-line summary of a record, which only --verbose asks for.

    Args:
        offset: The broker's position, independent of the exchange's numbering.
        frame: The typed frame handle() decoded.
        note: The sequence note handle() came back with.
        book: The book, for the quote a level2 frame's line carries.

    Returns:
        A line carrying the Kafka offset and what the decoded frame turned out to be.
    """
    if isinstance(frame, L2Frame):  # show the book filling up, frame by frame
        note = f" {book.quote()}{note}"
    return f"offset={offset} channel={channel_of(frame)} {summarise(frame)}{note}"


def summarise(frame: Frame) -> str:
    """Describe what one decoded frame carries, beyond the channel it came from.

    Args:
        frame: The typed frame decode_frame returned.

    Returns:
        The part of the summary line that differs from one frame type to the next.
    """
    match frame:  # a branch per type, which is what decoding into typed objects bought us
        case L2Frame():
            events = " ".join(
                f"type={event_type(event)} levels={len(event.updates)}" for event in frame.events
            )
            return f"seq={frame.sequence_num} {events}"
        case HeartbeatFrame():
            counters = " ".join(f"counter={tick.heartbeat_counter}" for tick in frame.events)
            return f"seq={frame.sequence_num} {counters}"
        case SubscriptionsFrame():
            active = " ".join(
                f"{name}={','.join(products)}"
                for event in frame.events
                for name, products in event.subscriptions.items()
            )
            return f"seq={frame.sequence_num} {active}"
        case ErrorFrame():
            return f"[exchange error] {frame.message}"
        case UnknownFrame():
            return f"seq={frame.sequence_num} [not modelled]"


def consume(verbose: bool, depth: int, interval_ms: float) -> None:
    """Read records, keep the book up to date, and publish its top until stopped.

    Args:
        verbose: True to print a summary line for every record, which is what this
            service used to do for every frame.
        depth: How many price levels to publish from each side of the book.
        interval_ms: Milliseconds between published records.
    """
    consumer = build_consumer()  # one consumer for the whole run, closed by the finally below
    watch = SequenceWatch()  # one tracker for the whole run, reported in the finally below
    book = OrderBook()  # one book for the whole run, reported in the finally below
    meter = Meter()  # one counter for the whole run, printed every few seconds
    publisher = Publisher(build_producer(), depth, interval_ms)  # one producer, drained below
    running = True  # cleared by the handler below, which is the only thing that ends this run

    def on_signal(signum: int, frame: types.FrameType | None) -> None:
        """Treat a stop request from Docker and a Ctrl+C the same way.

        Args:
            signum: Which signal arrived.
            frame: The interrupted stack frame, which is not used.
        """
        nonlocal running
        print(f"\n[stopping] signal {signum}")
        running = False  # the fetch waits at most one interval, then the loop below ends

    signal.signal(signal.SIGINT, on_signal)  # replaces the default that raises KeyboardInterrupt
    signal.signal(signal.SIGTERM, on_signal)  # without this Docker has to kill us outright
    consumer.subscribe([RAW_TOPIC])  # group subscription, so the broker assigns us the partitions
    print(f"subscribed to {RAW_TOPIC} at {BROKERS} as group {GROUP_ID}")
    print(f"publishing top {depth} to {BOOK_TOPIC} every {interval_ms:.0f}ms")
    try:
        while running:
            # The fetch waits only until the next record is owed, so the rhythm is kept
            # without a thread of its own; on a busy topic it returns sooner than that anyway.
            for message in consumer.consume(BATCH_SIZE, publisher.seconds_left()):
                error = message.error()
                if error is not None:  # a fetch problem must be visible, not swallowed
                    if error.code() != KafkaError._PARTITION_EOF:  # EOF is a marker, not a failure
                        print(f"[error] {error}")
                    continue
                frame, note = handle(message, watch, book, meter)
                if verbose and frame is not None:  # the old line-per-frame output, on request
                    print(describe(message.offset(), frame, note, book))
            publisher.publish_if_due(book)  # writes a record when one is owed, cheap when not
            if meter.due():  # outside the loop above, so a quiet topic still reports in
                print(f"{meter.report(consumer, book)} {publisher.report()}")
    finally:
        if meter.timed:  # skip a repeat line when the one above already covered everything
            print(f"{meter.report(consumer, book)} {publisher.report()}")  # asks for lag, so first
        pending = publisher.close()  # drain the queue while the broker connection is still up
        consumer.close()  # leave the group now rather than waiting out the session timeout
        elapsed = time.monotonic() - meter.started
        print(f"read {meter.read} records in {elapsed:.1f}s from {RAW_TOPIC} at {BROKERS}")
        print(f"published {publisher.published} to {BOOK_TOPIC} at {BROKERS}")
        if pending:  # the drain ran out of time, so some records never reached the broker
            print(f"[warn] {pending} records still undelivered after {FLUSH_TIMEOUT:.0f}s")
        if publisher.dropped:  # counted rather than raised, so one refusal never ends the run
            print(f"[warn] {publisher.dropped} records never reached the producer queue")
        print(watch.report())  # says plainly whether the run saw the whole stream
        print(book.status())  # and what the book ended up holding, as of which frame


def main() -> None:
    """Parse the command line and run one read."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="print a line for every record instead of a progress line every few seconds",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=DEPTH,
        help="price levels to publish from each side of the book",
    )
    parser.add_argument(
        "--interval-ms",
        type=float,
        default=INTERVAL_MS,
        help="milliseconds between published records",
    )
    args = parser.parse_args()
    if args.depth < 1:  # a depth of zero slices out a whole side rather than nothing at all
        parser.error("--depth must be 1 or more")
    if args.interval_ms <= 0:  # a rhythm of zero would spin the loop instead of pacing it
        parser.error("--interval-ms must be above zero")
    consume(args.verbose, args.depth, args.interval_ms)


if __name__ == "__main__":
    main()
