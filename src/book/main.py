"""Read the raw Coinbase Level 2 frames back off the Redpanda topic and summarise each one.

Frames are turned into typed objects by src.decode.messages and printed as a single
identifying line each -- the opening snapshot is megabytes, so dumping payloads is not
viable -- and nothing is written to disk. The book itself is not built here yet.

Every frame is also checked against the exchange's own sequence number, so a frame that
never reached us is named on the line where it went missing rather than passed on unnoticed.
"""

import argparse
import os  # reads the broker list from the environment
import time

import msgspec  # imported for the decode errors the stage 3 boundary raises
from confluent_kafka import Consumer, KafkaError, Message  # librdkafka client

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
GROUP_ID = "l2-book"  # the consumer group this reader joins
MAX_MESSAGES = 500  # exploration cap, paired with the producer's MAX_FRAMES
MAX_MESSAGE_BYTES = 20 * 1024 * 1024  # the opening snapshot runs ~5 MB, over the 1 MB default
IDLE_TIMEOUT = 20.0  # seconds of empty polls before concluding the topic has no more for us
POLL_TIMEOUT = 1.0  # seconds each poll waits before returning None
PAYLOAD_PREVIEW = 120  # characters of a malformed frame to show, so the log stays readable


def build_consumer() -> Consumer:
    """Create the consumer used to read raw frames back.

    The group never commits: with `enable.auto.commit` off and no manual commit, the group
    never acquires a stored position, so `auto.offset.reset` applies on every run and each run
    replays the same frames. That is what makes the spike reproducible. Committing is the
    change to make once the producer's frame cap goes away and a day of backlog is real.

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
        frame that arrived out of order, which the exchange says can be ignored, or a second
        capture run in the same topic. Either way the new value becomes the baseline, so the
        frames after it stay checkable.

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


def describe(message: Message, watch: SequenceWatch) -> str:
    """Build the one-line summary printed for a single record.

    Args:
        message: The record fetched from the topic.
        watch: The tracker, updated with whatever counter this frame turned out to carry.

    Returns:
        A line carrying the Kafka offset and what the decoded frame turned out to be.
    """
    offset = message.offset()  # the broker's position, independent of the exchange's numbering
    try:  # EAFP: decode, and only deal with the frames stage 3 refuses if there are any
        frame = decode_frame(message.value())
    except msgspec.ValidationError as error:  # listed first, because it subclasses DecodeError
        return f"offset={offset} [invalid] {error}"  # refused, so it never reaches a book
    except msgspec.DecodeError:  # not JSON at all, so there is nothing to report but bytes
        preview = message.value()[:PAYLOAD_PREVIEW].decode("utf-8", errors="replace")
        return f"offset={offset} [unparsed] {preview}"
    note = watch.check(frame)  # empty unless this frame's counter skipped ahead
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


def consume(max_messages: int, idle_timeout: float) -> None:
    """Read records from the topic until the cap is reached or the topic goes quiet.

    Args:
        max_messages: How many records to read before stopping.
        idle_timeout: Seconds of empty polls to tolerate before concluding there is no more.
    """
    consumer = build_consumer()  # one consumer for the whole run, closed by the finally below
    watch = SequenceWatch()  # one tracker for the whole replay, reported in the finally below
    consumer.subscribe([TOPIC])  # group subscription, so the broker assigns us the partitions
    print(f"subscribed to {TOPIC} at {BROKERS} as group {GROUP_ID}")
    read = 0  # records actually summarised, which is what the closing total reports
    reason = "idle"  # overwritten when the cap is what stops us, for the closing line
    started = time.monotonic()
    last_record = time.monotonic()  # idle clock, reset on every real record
    try:
        while read < max_messages:
            message = consumer.poll(POLL_TIMEOUT)
            if message is None:  # nothing ready this round; the topic may simply be exhausted
                if time.monotonic() - last_record > idle_timeout:
                    break  # without this an under-full topic would poll forever
                continue
            error = message.error()
            if error is not None:  # a fetch problem must be visible, not swallowed
                if error.code() != KafkaError._PARTITION_EOF:  # EOF is a marker, not a failure
                    print(f"[error] {error}")
                continue
            print(describe(message, watch))
            read += 1
            last_record = time.monotonic()  # a real record restarts the idle clock
        else:  # the while condition failed, so the cap is what ended the loop
            reason = "cap"
    except KeyboardInterrupt:
        print("\n[interrupted] closing early")
        reason = "interrupted"
    finally:
        consumer.close()  # leave the group now rather than waiting out the session timeout
        elapsed = time.monotonic() - started
        print(f"read {read} records in {elapsed:.1f}s from {TOPIC} at {BROKERS}")
        print(watch.report())  # says plainly whether the run saw the whole stream
        if reason == "idle":  # say so, because a short run otherwise looks like a failure
            print(f"[done] stopped after {idle_timeout:.0f}s with no new records")


def main() -> None:
    """Parse the command line and run one read."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--max-messages",
        type=int,
        default=MAX_MESSAGES,
        help="records to read before stopping",
    )
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=IDLE_TIMEOUT,
        help="seconds without a record before giving up",
    )
    args = parser.parse_args()
    consume(args.max_messages, args.idle_timeout)


if __name__ == "__main__":
    main()
