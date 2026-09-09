"""Publish the Coinbase Advanced Trade Level 2 feed for BTC-USD to a Redpanda topic.

Everything for this service lives in this one file. Frames are forwarded to the topic
exactly as they arrived, one Kafka record per frame, and nothing is written to disk.

The capture runs until something stops it. A lost connection is dialled again instead of
ending the run, and a stop request drains the producer queue before the process exits.
"""

import argparse
import json
import os  # reads the broker list from the environment
import signal  # lets a stop request from Docker reach the code below
import threading
import time
import types  # only for the stack frame a signal handler is handed

import websocket
from confluent_kafka import KafkaError, KafkaException, Message, Producer  # librdkafka client

WS_URL = "wss://advanced-trade-ws.coinbase.com"
PRODUCT_ID = "BTC-USD"
TOPIC = "l2.raw.btc-usd"  # must match the topic `topic-init` creates in docker-compose.yml
BROKERS = os.environ.get("REDPANDA_BROKERS", "redpanda-0:9092")  # host runs use localhost:19092
MAX_MESSAGE_BYTES = 8 * 1024 * 1024  # the opening snapshot runs ~5 MB, over the 1 MB default
FLUSH_TIMEOUT = 10.0  # short enough that the drain finishes inside the compose grace period
DEFAULT_DURATION = 0.0  # zero means keep capturing until something asks us to stop
PING_INTERVAL = 20
PING_TIMEOUT = 10
RECONNECT_DELAY = 5.0  # how long to wait after a lost connection before dialling again
RECONNECT_NAP = 0.2  # length of each nap inside that wait, so a stop request lands quickly
STATS_INTERVAL = 10.0  # seconds between progress lines, which stand in for the per-frame ones


def build_producer() -> Producer:
    """Create the producer used to publish raw frames.

    librdkafka already is the queue-and-thread design: produce() copies the payload into an
    in-memory queue and returns, while a background C thread batches and writes to the socket.
    The queue.buffering.max.messages / .max.kbytes defaults (100k frames, ~1 GiB) are the
    backlog that rides out a broker hiccup, so no buffering of our own is needed.

    The size limit is raised because the first frame of the feed is a full book snapshot,
    which is thousands of times larger than the incremental updates that follow it.

    Returns:
        A producer configured to talk to the Redpanda broker.
    """
    return Producer(  # config is a plain dict of librdkafka property names
        {
            "bootstrap.servers": BROKERS,  # where to find the cluster
            "linger.ms": 5,  # wait this long to gather more frames into one produce request
            "compression.type": "lz4",  # per batch, where near-identical JSON compresses well
            "acks": "all",  # only count a frame delivered once the broker has it durably
            "enable.idempotence": True,  # a retry cannot reorder or duplicate frames
            "message.max.bytes": MAX_MESSAGE_BYTES,  # must match the topic's max.message.bytes
        }
    )


def capture(duration: float) -> None:
    """Connect, subscribe, and publish frames until the capture is stopped.

    Args:
        duration: Seconds to capture before stopping on purpose, or zero to keep going until
            a stop request arrives.
    """
    producer = build_producer()
    stats = {"frames": 0, "dropped": 0, "connects": 0, "reported": 0}
    running = True  # cleared by halt(), which is the only thing that ends this run
    app: websocket.WebSocketApp | None = None  # the live connection, replaced on every dial
    started = time.monotonic()
    last_report = started  # when the most recent progress line went out

    def close_socket() -> None:
        """Close the live connection, which is what wakes the waiting socket read."""
        if app is not None:  # nothing to close before the first dial, or after the last one
            app.close()

    def halt() -> None:
        """End the run for good, whether a stop request or the duration timer asked for it.

        The close is handed to a thread of its own on purpose. A signal handler runs on the
        main thread, interrupting the socket read already in progress there, and closing from
        inside that read leaves run_forever waiting for good -- measured as a hang, not a
        slow exit. Off the main thread the same close returns in a few hundredths of a second.
        """
        nonlocal running
        if not running:  # a second stop request has nothing left to do
            return
        running = False  # stops the dial loop below from opening another connection
        threading.Thread(target=close_socket, daemon=True).start()

    def on_signal(signum: int, frame: types.FrameType | None) -> None:
        """Treat a stop request from Docker and a Ctrl+C the same way.

        Args:
            signum: Which signal arrived.
            frame: The interrupted stack frame, which is not used.
        """
        print(f"\n[stopping] signal {signum}")
        halt()

    signal.signal(signal.SIGINT, on_signal)  # replaces the default that raises KeyboardInterrupt
    signal.signal(signal.SIGTERM, on_signal)  # without this Docker has to kill us outright

    def report(final: bool = False) -> None:
        """Print one progress line, at most one every STATS_INTERVAL seconds.

        Args:
            final: True to print whatever the clock says, for the line at shutdown.
        """
        nonlocal last_report
        now = time.monotonic()
        if not final and now - last_report < STATS_INTERVAL:  # too soon, so this costs nothing
            return
        window = now - last_report
        fresh = stats["frames"] - stats["reported"]  # frames since the previous line, not in total
        rate = fresh / window if window else 0.0
        last_report, stats["reported"] = now, stats["frames"]
        print(  # queue is the producer's own backlog; a rising one means the broker is behind
            f"[stats] up {now - started:.0f}s connects {stats['connects']} "
            f"frames {stats['frames']} ({rate:.0f}/s) dropped {stats['dropped']} "
            f"queue {len(producer)}"
        )

    def on_delivery(error: KafkaError | None, message: Message) -> None:
        """Report a frame that never landed; a delivered one is counted, not printed.

        Args:
            error: The broker-side failure, or None when the frame landed.
            message: The delivered record, carrying its partition and offset.
        """
        if error is not None:  # a frame that never landed must be visible, not swallowed
            print(f"[delivery failed] {error}")

    def on_open(ws: websocket.WebSocketApp) -> None:
        """Send our subscriptions as soon as the socket is connected."""
        heartbeats = {"type": "subscribe", "channel": "heartbeats"}
        ws.send(json.dumps(heartbeats))
        level2 = {"type": "subscribe", "product_ids": [PRODUCT_ID], "channel": "level2"}
        ws.send(json.dumps(level2))
        print(f"subscribed to heartbeats and level2 for {PRODUCT_ID}")

    def on_message(ws: websocket.WebSocketApp, message: str) -> None:
        """Publish one incoming frame, and now and then say how the capture is doing."""
        payload = message.encode("utf-8")
        try:  # EAFP: enqueue, and only deal with a full queue if it actually happens
            producer.produce(TOPIC, key=PRODUCT_ID, value=payload, on_delivery=on_delivery)
        except BufferError:
            producer.poll(1.0)
            try:  # one retry; a second failure means the backlog is real, not a momentary spike
                producer.produce(TOPIC, key=PRODUCT_ID, value=payload, on_delivery=on_delivery)
            except BufferError:
                stats["dropped"] += 1
                print("[warn] producer queue full, frame dropped")
        except KafkaException as error:
            stats["dropped"] += 1
            print(f"[warn] frame rejected: {error}")
        stats["frames"] += 1
        producer.poll(0)
        report()  # returns straight away unless the interval has passed, so this stays cheap

    def on_error(ws: websocket.WebSocketApp, error: Exception) -> None:
        """Report a socket-level error, staying quiet about one our own stop caused.

        Args:
            ws: The connection that failed.
            error: What went wrong.
        """
        if running:  # after halt() this is just our own close arriving, which is not a fault
            print(f"[error] {error}")

    def on_close(ws: websocket.WebSocketApp, status_code: int | None, msg: str | None) -> None:
        """Report that the connection has ended."""
        print(f"[closed] status={status_code} message={msg}")

    timer = threading.Timer(duration, halt) if duration > 0 else None  # armed for a timed run only
    if timer is not None:
        timer.start()
    try:
        while running:  # one turn of this loop is one connection to the exchange
            app = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            stats["connects"] += 1  # counted so the progress line shows how often we redialled
            app.run_forever(ping_interval=PING_INTERVAL, ping_timeout=PING_TIMEOUT)
            if not running:  # the socket closed because we asked it to, so do not dial again
                break
            print(f"[reconnect] connection lost, retrying in {RECONNECT_DELAY:.0f}s")
            deadline = time.monotonic() + RECONNECT_DELAY  # when the next dial becomes due
            while running and time.monotonic() < deadline:  # short naps, so a stop lands fast
                time.sleep(RECONNECT_NAP)
    finally:
        if timer is not None:  # nothing to cancel when the run had no duration
            timer.cancel()
        report(final=True)  # printed before the flush, so queue shows the backlog at stop time
        pending = producer.flush(FLUSH_TIMEOUT)  # the one deliberate block: drain before exiting
        elapsed = time.monotonic() - started
        published = stats["frames"] - stats["dropped"]
        print(f"captured {stats['frames']} frames in {elapsed:.1f}s")
        print(f"published {published} to {TOPIC} at {BROKERS}")
        if pending:
            print(f"[warn] {pending} messages still undelivered after {FLUSH_TIMEOUT:.0f}s")
        if stats["dropped"]:
            print(f"[warn] {stats['dropped']} frames never reached the producer queue")


def main() -> None:
    """Parse the command line and run one capture."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--duration",
        type=float,
        default=DEFAULT_DURATION,
        help="seconds to capture before stopping, or 0 to run until stopped",
    )
    args = parser.parse_args()
    capture(args.duration)


if __name__ == "__main__":
    main()
