"""Read the raw Coinbase Level 2 frames back off the Redpanda topic and summarise each one.

This is an exploration spike: everything lives in this one file. Frames are printed as a single
identifying line each -- the opening snapshot is megabytes, so dumping payloads is not viable --
and nothing is decoded into typed objects or written to disk.
"""

import argparse
import json
import os  # reads the broker list from the environment
import time

from confluent_kafka import Consumer, KafkaError, Message  # librdkafka client

TOPIC = "l2.raw.btc-usd"  # must match the topic `topic-init` creates in docker-compose.yml
BROKERS = os.environ.get("REDPANDA_BROKERS", "redpanda-0:9092")  # host runs use localhost:19092
GROUP_ID = "l2-consume"  # the consumer group this spike joins
MAX_MESSAGES = 500  # exploration cap, paired with the producer's MAX_FRAMES
MAX_MESSAGE_BYTES = 20 * 1024 * 1024  # the opening snapshot runs ~5 MB, over the 1 MB default
IDLE_TIMEOUT = 10.0  # seconds of empty polls before concluding the topic has no more for us
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


def describe(message: Message) -> str:
    """Build the one-line summary printed for a single record.

    Args:
        message: The record fetched from the topic.

    Returns:
        A line carrying the Kafka offset and the frame's own sequence number.
    """
    offset = message.offset()  # the broker's position, independent of the exchange's numbering
    try:  # EAFP: parse, and only deal with non-JSON if the topic actually holds some
        frame = json.loads(message.value())
    except json.JSONDecodeError:  # keep the loop alive; a bad frame is data, not a crash
        preview = message.value()[:PAYLOAD_PREVIEW].decode("utf-8", errors="replace")
        return f"offset={offset} [unparsed] {preview}"
    sequence = frame.get("sequence_num")  # top level on every Coinbase frame
    channel = frame.get("channel")  # l2_data for book traffic, subscriptions for the ack
    line = f"offset={offset} seq={sequence} channel={channel}"
    events = frame.get("events") or []  # the subscriptions ack has events but no book events
    event = events[0] if events else {}
    if "type" in event:  # only book frames carry a type and a batch of price-level updates
        updates = event.get("updates") or []
        line += f" type={event['type']} updates={len(updates)}"
    return line


def consume(max_messages: int, idle_timeout: float) -> None:
    """Read records from the topic until the cap is reached or the topic goes quiet.

    Args:
        max_messages: How many records to read before stopping.
        idle_timeout: Seconds of empty polls to tolerate before concluding there is no more.
    """
    consumer = build_consumer()  # one consumer for the whole run, closed by the finally below
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
            print(describe(message))
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
