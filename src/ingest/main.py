"""Publish the Coinbase Advanced Trade Level 2 feed for BTC-USD to a Redpanda topic.

This is an exploration spike: everything lives in this one file. Frames are forwarded to the
topic exactly as they arrived, one Kafka record per frame, and nothing is written to disk.
"""

import argparse
import json
import os  # reads the broker list from the environment
import threading
import time

import websocket
from confluent_kafka import KafkaError, KafkaException, Message, Producer  # librdkafka client

WS_URL = "wss://advanced-trade-ws.coinbase.com"
PRODUCT_ID = "BTC-USD"
TOPIC = "l2.raw.btc-usd"  # must match the topic `topic-init` creates in docker-compose.yml
BROKERS = os.environ.get("REDPANDA_BROKERS", "redpanda-0:9092")  # host runs use localhost:19092
MAX_FRAMES = 500  # exploration cap; delete this and its test in on_message for a real capture
MAX_MESSAGE_BYTES = 8 * 1024 * 1024  # the opening snapshot runs ~5 MB, over the 1 MB default
FLUSH_TIMEOUT = 30.0  # seconds to wait on shutdown for the producer queue to drain
DEFAULT_DURATION = 20.0
PING_INTERVAL = 20
PING_TIMEOUT = 10


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
    """Connect, subscribe, and publish frames until the cap or the time is up.

    Args:
        duration: How many seconds to stay connected before closing.
    """
    producer = build_producer()  # one producer for the whole run, closed by the flush below
    stats = {"frames": 0, "dropped": 0}  # a dict so the inner callbacks can mutate the counts

    def on_delivery(error: KafkaError | None, message: Message) -> None:
        """Report the fate of one published frame.

        Args:
            error: The broker-side failure, or None when the frame landed.
            message: The delivered record, carrying its partition and offset.
        """
        if error is not None:  # a frame that never landed must be visible, not swallowed
            print(f"[delivery failed] {error}")
            return
        # Per-frame printing only makes sense at this scale; drop it with MAX_FRAMES.
        print(f"delivered partition={message.partition()} offset={message.offset()}")

    def on_open(ws: websocket.WebSocketApp) -> None:
        """Send our subscriptions as soon as the socket is connected."""
        # heartbeats = {"type": "subscribe", "channel": "heartbeats"}
        # ws.send(json.dumps(heartbeats))
        level2 = {"type": "subscribe", "product_ids": [PRODUCT_ID], "channel": "level2"}
        ws.send(json.dumps(level2))
        print(f"subscribed to level2 for {PRODUCT_ID}")

    def on_message(ws: websocket.WebSocketApp, message: str) -> None:
        """Publish one incoming frame, closing the socket once the cap is reached."""
        payload = message.encode("utf-8")  # produce() takes bytes; the frame text is untouched
        try:  # EAFP: enqueue, and only deal with a full queue if it actually happens
            producer.produce(TOPIC, key=PRODUCT_ID, value=payload, on_delivery=on_delivery)
        except BufferError:  # the queue is full, so the broker is down or badly lagging
            producer.poll(1.0)  # give the background thread a moment to drain it
            try:  # one retry; a second failure means the backlog is real, not a momentary spike
                producer.produce(TOPIC, key=PRODUCT_ID, value=payload, on_delivery=on_delivery)
            except BufferError:  # drop rather than block, which would stall the socket reader
                stats["dropped"] += 1
                print("[warn] producer queue full, frame dropped")
        except KafkaException as error:  # rejected outright, e.g. a frame over max.message.bytes
            stats["dropped"] += 1  # count it, so the published total below stays honest
            print(f"[warn] frame rejected: {error}")
        stats["frames"] += 1  # count every frame the exchange handed us, delivered or not
        producer.poll(0)  # serve delivery callbacks without blocking this reader thread
        if stats["frames"] >= MAX_FRAMES:  # exploration cap; remove alongside MAX_FRAMES
            ws.close()

    def on_error(ws: websocket.WebSocketApp, error: Exception) -> None:
        """Report a socket-level error, treating Ctrl+C as a normal stop."""
        if isinstance(error, KeyboardInterrupt):
            print("\n[interrupted] closing early")
            return
        print(f"[error] {error}")

    def on_close(ws: websocket.WebSocketApp, status_code: int | None, msg: str | None) -> None:
        """Report that the connection has ended."""
        print(f"[closed] status={status_code} message={msg}")

    app = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    timer = threading.Timer(duration, app.close)
    started = time.monotonic()
    timer.start()
    try:
        app.run_forever(ping_interval=PING_INTERVAL, ping_timeout=PING_TIMEOUT)
    except KeyboardInterrupt:
        print("\n[interrupted] closing early")
        app.close()
    finally:
        timer.cancel()
        pending = producer.flush(FLUSH_TIMEOUT)  # the one deliberate block: drain before exiting
        elapsed = time.monotonic() - started
        published = stats["frames"] - stats["dropped"]  # every frame not dropped reached the queue
        print(f"captured {stats['frames']} frames in {elapsed:.1f}s")
        print(f"published {published} to {TOPIC} at {BROKERS}")
        if pending:  # flush timed out, so some frames never left the queue
            print(f"[warn] {pending} messages still undelivered after {FLUSH_TIMEOUT:.0f}s")
        if stats["dropped"]:  # surface loss instead of letting the totals hide it
            print(f"[warn] {stats['dropped']} frames never reached the producer queue")


def main() -> None:
    """Parse the command line and run one capture."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--duration",
        type=float,
        default=DEFAULT_DURATION,
        help="seconds to capture before closing the connection",
    )
    args = parser.parse_args()
    capture(args.duration)


if __name__ == "__main__":
    main()
