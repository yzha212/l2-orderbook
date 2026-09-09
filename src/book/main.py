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
"""

import argparse
import os  # reads the broker list from the environment
import signal  # lets a stop request from Docker reach the code below
import time
import types  # only for the stack frame a signal handler is handed

import msgspec  # imported for the decode errors the stage 3 boundary raises
from confluent_kafka import Consumer, KafkaError, KafkaException, Message  # librdkafka client

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

TOPIC = "l2.raw.btc-usd"  # must match the topic `topic-init` creates in docker-compose.yml
BROKERS = os.environ.get("REDPANDA_BROKERS", "redpanda-0:9092")  # host runs use localhost:19092
GROUP_ID = os.environ.get("CONSUMER_GROUP", "l2-book")  # compose sets it; default for host runs
MAX_MESSAGE_BYTES = 20 * 1024 * 1024  # the opening snapshot runs ~5 MB, over the 1 MB default
POLL_TIMEOUT = 1.0  # seconds each fetch waits before coming back with whatever it has
BATCH_SIZE = 100  # records asked for per fetch, so catching up costs fewer round trips
STATS_INTERVAL = 5.0  # seconds between progress lines
PAYLOAD_PREVIEW = 120  # characters of a malformed frame to show, so the log stays readable


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

    The timings are the point of this class. `apply` covers the two full scans of a book side
    that check_crossed still costs per frame, so a figure that climbs as the book fills is
    what says the plain dicts need replacing with a sorted structure. `lag` is the plainer
    test: as long as it stays near zero the reader is keeping up with the exchange.

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


def consume(verbose: bool) -> None:
    """Read records from the topic and keep the book up to date until stopped.

    Args:
        verbose: True to print a summary line for every record, as the spike used to.
    """
    consumer = build_consumer()  # one consumer for the whole run, closed by the finally below
    watch = SequenceWatch()  # one tracker for the whole run, reported in the finally below
    book = OrderBook()  # one book for the whole run, reported in the finally below
    meter = Meter()  # one counter for the whole run, printed every few seconds
    running = True  # cleared by the handler below, which is the only thing that ends this run

    def on_signal(signum: int, frame: types.FrameType | None) -> None:
        """Treat a stop request from Docker and a Ctrl+C the same way.

        Args:
            signum: Which signal arrived.
            frame: The interrupted stack frame, which is not used.
        """
        nonlocal running
        print(f"\n[stopping] signal {signum}")
        running = False  # the fetch below returns within POLL_TIMEOUT, then the loop ends

    signal.signal(signal.SIGINT, on_signal)  # replaces the default that raises KeyboardInterrupt
    signal.signal(signal.SIGTERM, on_signal)  # without this Docker has to kill us outright
    consumer.subscribe([TOPIC])  # group subscription, so the broker assigns us the partitions
    print(f"subscribed to {TOPIC} at {BROKERS} as group {GROUP_ID}")
    try:
        while running:
            for message in consumer.consume(BATCH_SIZE, POLL_TIMEOUT):  # empty list when quiet
                error = message.error()
                if error is not None:  # a fetch problem must be visible, not swallowed
                    if error.code() != KafkaError._PARTITION_EOF:  # EOF is a marker, not a failure
                        print(f"[error] {error}")
                    continue
                frame, note = handle(message, watch, book, meter)
                if verbose and frame is not None:  # the old line-per-frame output, on request
                    print(describe(message.offset(), frame, note, book))
            if meter.due():  # outside the loop above, so a quiet topic still reports in
                print(meter.report(consumer, book))
    finally:
        if meter.timed:  # skip a repeat line when the one above already covered everything
            print(meter.report(consumer, book))  # asks the consumer for lag, so it goes first
        consumer.close()  # leave the group now rather than waiting out the session timeout
        elapsed = time.monotonic() - meter.started
        print(f"read {meter.read} records in {elapsed:.1f}s from {TOPIC} at {BROKERS}")
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
    args = parser.parse_args()
    consume(args.verbose)


if __name__ == "__main__":
    main()
