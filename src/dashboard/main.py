"""Show the top of the live order book in the terminal.

This is stage 6 of the architecture. It reads the records the book builder publishes to
l2.book.btc-usd and repaints a price ladder ten times a second, so the in-memory book can
be checked by eye against what a real order book should look like.

It is a view and nothing else. It writes to no topic, commits no offset, and takes no input
from the keyboard. It starts at the newest record, because a dashboard wants the book as it
is now rather than an hour of replay, and it takes the topic's one partition directly rather
than joining a consumer group, so that several people can watch the same book at once.

Each repaint also shows how old the book is. Two numbers rather than one, because a single
figure would hide where a delay came from: `book age` is the gap the book builder itself
saw between the exchange's frame and publishing it, and `total` adds the topic and this
screen on top of that.
"""

import os  # reads the broker list and the group name from the environment
import signal  # lets a stop request from Docker reach the loop below
import types  # only for the stack frame a signal handler is handed
from datetime import UTC, datetime
from itertools import zip_longest  # pairs the two sides even when one is shorter

import msgspec  # decodes the published record back into the struct the book builder wrote
from confluent_kafka import OFFSET_END, Consumer, KafkaError, Message, TopicPartition
from rich.console import Console
from rich.live import Live  # repaints one region of the terminal instead of scrolling
from rich.table import Table

from src.book.book import TopLevels  # stage 4's published record; the contract for this view

BOOK_TOPIC = "l2.book.btc-usd"  # must match the topic `topic-init` creates in docker-compose.yml
PARTITION = 0  # the book topic is created with exactly one, and this reader takes it directly
BROKERS = os.environ.get("REDPANDA_BROKERS", "redpanda-0:9092")  # host runs use localhost:19092
GROUP_ID = os.environ.get("CONSUMER_GROUP", "l2-dashboard")  # compose sets it; default for host runs
PRODUCT_ID = "BTC-USD"  # the one symbol this project follows, shown in the header
FPS = 10  # frames a second, which is what section 6 of the architecture asks for
BATCH_SIZE = 20  # records asked for per fetch: one tick's worth at ten a second, with room over

_decoder = msgspec.json.Decoder(TopLevels)  # built once, as stage 3 does with its decoders


def build_consumer() -> Consumer:
    """Create the consumer used to read the published book.

    Returns:
        A consumer configured to talk to the Redpanda broker.
    """
    return Consumer(  # config is a plain dict of librdkafka property names
        {
            "bootstrap.servers": BROKERS,  # where to find the cluster
            "group.id": GROUP_ID,  # the client insists on one; see watch() for why nothing uses it
            "enable.auto.commit": False,  # nothing to resume, so nothing to remember
        }
    )


def newest(messages: list[Message]) -> TopLevels | None:
    """Return the most recent record in one fetch and drop the rest.

    This is what keeps the screen current. Ten records a second arriving into a ten frame a
    second screen means one record per frame while things are calm, but a pause of any kind
    leaves a backlog, and painting that backlog frame by frame would put the view further
    behind with every tick. Only the last record describes the book as it is now.

    Args:
        messages: Whatever the fetch came back with, newest last.

    Returns:
        The last record that could be decoded, or None when the fetch was empty or carried
        nothing usable.
    """
    for message in reversed(messages):  # walk back from the newest, stop at the first good one
        error = message.error()
        if error is not None:  # a fetch problem must be visible, not swallowed
            if error.code() != KafkaError._PARTITION_EOF:  # EOF is a marker, not a failure
                print(f"[error] {error}")
            continue
        try:  # EAFP: decode, and only deal with a record stage 4 wrote badly if there is one
            return _decoder.decode(message.value())
        except msgspec.ValidationError as error:  # listed first, because it subclasses DecodeError
            print(f"offset={message.offset()} [invalid] {error}")
        except msgspec.DecodeError:  # not JSON at all, so there is nothing to report but that
            print(f"offset={message.offset()} [unparsed]")
    return None


def gap(later: datetime, earlier: datetime | None) -> str:
    """Describe the distance between two moments in milliseconds.

    Args:
        later: The moment further on.
        earlier: The moment to measure back to, or None when the record carried no timestamp.

    Returns:
        The gap in milliseconds, or a dash when there is nothing to measure against.
    """
    if earlier is None:  # a record published before any frame applied, which the types allow
        return "-"
    return f"{(later - earlier).total_seconds() * 1000:.0f}ms"


def spread(record: TopLevels) -> str:
    """Describe the gap between the best ask and the best bid.

    Args:
        record: The published record to read the top of each side from.

    Returns:
        The spread, or a dash while either side is empty.
    """
    try:  # EAFP: an empty side raises here rather than being checked for first
        return f"{record.asks[0][0] - record.bids[0][0]}"
    except IndexError:
        return "-"


def render(record: TopLevels | None, now: datetime) -> Table:
    """Draw one frame of the ladder.

    The clock is passed in rather than read here, which is what keeps this function pure:
    given the same record and the same moment it always draws the same table, so it can be
    checked without a broker, a terminal or a wait.

    Args:
        record: The most recent published record, or None before the first one arrives.
        now: The moment this frame is being drawn, used for the total latency.

    Returns:
        The table to hand to the live display.
    """
    table = Table(expand=False)  # a fixed ladder, so it sits at its natural width
    table.add_column("bid size", justify="right", style="green")  # the two sides read outward
    table.add_column("bid price", justify="right", style="green")  # from the spread in the middle
    table.add_column("ask price", justify="right", style="red")
    table.add_column("ask size", justify="right", style="red")
    if record is None:  # before the book builder has loaded a snapshot there is nothing to draw
        table.title = f"{PRODUCT_ID}  waiting for the first record"
        table.caption = "doubtful: -"
        return table
    for bid, ask in zip_longest(record.bids, record.asks):  # None fills whichever side runs out
        table.add_row(
            *(("", "") if bid is None else (f"{bid[1]}", f"{bid[0]}")),
            *(("", "") if ask is None else (f"{ask[0]}", f"{ask[1]}")),
        )
    table.title = f"{PRODUCT_ID}  seq {record.sequence_num}"
    table.caption = (
        f"spread {spread(record)}   "
        f"book age {gap(record.published_at, record.timestamp)}   "  # what stage 4 itself saw
        f"total {gap(now, record.timestamp)}\n"  # plus the topic and this screen, on our clock
        f"doubtful: {str(record.doubtful).lower()}"  # printed as the record carries it, no more
    )
    return table


def watch() -> None:
    """Read the published book and repaint the ladder until stopped."""
    consumer = build_consumer()  # one consumer for the whole run, closed by the finally below
    console = Console()  # detects whether it has a real terminal, which decides the live refresh
    record: TopLevels | None = None  # the newest record seen, held over when a fetch comes back empty
    read = 0  # records that reached us, most of which are never drawn
    running = True  # cleared by the handler below, which is the only thing that ends this run

    def on_signal(signum: int, frame: types.FrameType | None) -> None:
        """Treat a stop request from Docker and a Ctrl+C the same way.

        Args:
            signum: Which signal arrived.
            frame: The interrupted stack frame, which is not used.
        """
        nonlocal running
        running = False  # the fetch waits at most one frame, then the loop below ends

    signal.signal(signal.SIGINT, on_signal)  # replaces the default that raises KeyboardInterrupt
    signal.signal(signal.SIGTERM, on_signal)  # without this Docker has to kill us outright
    # Take the partition outright instead of subscribing to a group, which is the one place
    # this reader parts from src/book/main.py on purpose. A group hands each partition to
    # exactly one member and this topic has exactly one partition, so a second dashboard --
    # or a first one that was killed before it could leave the group -- holds nothing and
    # draws an empty screen with no error to explain it. That is not hypothetical: two runs
    # left over from one test session sat in the group together, and the newer one showed
    # "waiting for the first record" for forty seconds while the topic filled at ten records
    # a second. Assigned directly, any number of dashboards can watch the same book and none
    # of them waits out a rebalance first. OFFSET_END is what `auto.offset.reset` would have
    # meant: start at the newest record, because an hour-old book is worse than no book.
    consumer.assign([TopicPartition(BOOK_TOPIC, PARTITION, OFFSET_END)])
    print(f"reading {BOOK_TOPIC} partition {PARTITION} at {BROKERS} from the newest record")
    print(f"showing the top of the book {FPS} times a second; Ctrl+C to stop")
    try:
        # Built once, outside the loop: a new Live per tick clears and reprints its region,
        # which is what makes a display scroll and flicker instead of updating in place.
        with Live(render(None, datetime.now(UTC)), console=console, refresh_per_second=FPS) as live:
            while running:
                # The fetch waits at most one frame, so a silent topic still repaints on time
                # and the total latency below keeps climbing to show the feed has gone quiet.
                messages = consumer.consume(BATCH_SIZE, 1 / FPS)
                read += sum(1 for message in messages if message.error() is None)
                record = newest(messages) or record  # hold the last one when nothing came back
                live.update(render(record, datetime.now(UTC)))
    finally:
        consumer.close()  # leave the group now rather than waiting out the session timeout
        print(f"read {read} records from {BOOK_TOPIC} at {BROKERS}")
        if record is not None:  # the last frame drawn, kept as evidence once the screen is gone
            print(f"[book] last seq={record.sequence_num} at {record.timestamp}")
            print(f"[book] spread={spread(record)} doubtful: {str(record.doubtful).lower()}")


if __name__ == "__main__":
    watch()
