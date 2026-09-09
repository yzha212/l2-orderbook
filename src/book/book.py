"""Hold the live Level 2 order book for BTC-USD in memory.

This is stage 4 of the architecture. Like src/decode/messages.py it is a library rather
than a service -- there is no Dockerfile and no main here; the reader imports it. Keeping
the Kafka consumer out means the book can be driven straight from decoded frames.

Prices and sizes arrive as strings and become Decimal here. Strings cannot be ordered as
numbers ("9" sorts above "10") and floats cannot hold a price like 111234.57 exactly, so
Decimal is the only type that both sorts and compares correctly. Nothing is lost: the
decode boundary hands over the exact text the exchange sent, and Decimal keeps it exact.

Each side is a SortedDict rather than a plain dict, so the best price is whichever key sits
at the end and never has to be searched for. On a plain dict the best bid and ask were a
scan of every level, which is what the reader spent nearly all its time doing.
"""

from datetime import datetime
from decimal import Decimal

from sortedcontainers import SortedDict  # holds each side in price order, best price at an end

from src.decode.messages import L2Frame, PriceLevel, Snapshot, Update

ZERO = Decimal(0)  # built once, because every level applied compares against it


class OrderBook:
    """The full bid and ask book, loaded from a snapshot and edited by updates.

    A snapshot replaces the whole book; an update edits the levels it names, and a size of
    zero removes one. The book is only as good as the stream that fed it, so the counters
    below record every doubt raised along the way rather than hiding it.

    Attributes:
        bids: Resting buy levels, price to size, held in ascending price order.
        asks: Resting sell levels, price to size, in that same order. The exchange calls
            this side "offer".
        timestamp: When the exchange sent the most recent frame applied, None until the
            first one lands.
        sequence_num: That same frame's counter, so the timestamp can be traced back to a
            frame in the log.
        frames: Frames applied to the book.
        snapshots: Snapshots loaded. Zero means nothing is loaded yet and updates cannot
            be trusted.
        skipped: Update events dropped because no snapshot had been loaded yet.
        doubtful: Set by the caller when the stream lost a frame, so a book that may have
            drifted says so.
        crossings: Times the best bid landed at or above the best ask.
        negatives: Levels the exchange sent with a size below zero.
    """

    def __init__(self) -> None:
        """Start with an empty book that has loaded nothing and seen no faults."""
        self.bids: SortedDict = SortedDict()  # price -> size, held in price order
        self.asks: SortedDict = SortedDict()
        self.timestamp: datetime | None = None  # None is what "no frame applied yet" means
        self.sequence_num: int | None = None
        self.frames = 0
        self.snapshots = 0
        self.skipped = 0
        self.doubtful = False
        self.crossings = 0
        self.negatives = 0

    @property
    def loaded(self) -> bool:
        """Whether a snapshot has been loaded, which is what makes updates meaningful.

        Returns:
            True once at least one snapshot has been applied.
        """
        return self.snapshots > 0

    def apply(self, frame: L2Frame) -> None:
        """Apply one level2 frame to the book.

        The timestamp and sequence number are assigned only after every event in the frame
        has landed, so a reader can never catch a half-applied frame wearing a fresh
        timestamp. That is what makes the stored timestamp mean "the book is complete as of
        this frame", which is what the 100 ms publisher will need.

        Args:
            frame: The decoded level2 frame to apply.
        """
        for event in frame.events:
            match event:  # a branch per type, which is why stage 3 keeps them apart
                case Snapshot():
                    self.bids.clear()  # a snapshot replaces the book rather than adding to it
                    self.asks.clear()
                    self.snapshots += 1
                    self._load(event.updates)
                case Update():
                    if not self.loaded:  # edits to a book we never loaded would invent one
                        self.skipped += 1
                        continue
                    self._load(event.updates)
        self.frames += 1
        self.timestamp = frame.timestamp  # the goal: the last frame processed, in one place
        self.sequence_num = frame.sequence_num
        self.check_crossed()  # two end lookups on a sorted side, so the guarantee is near free

    def _load(self, levels: list[PriceLevel]) -> None:
        """Apply a batch of price levels to whichever side each one names.

        Args:
            levels: The levels carried by one snapshot or update event.
        """
        for level in levels:
            side = self.bids if level.side == "bid" else self.asks  # "offer" is the ask side
            price = Decimal(level.price_level)
            size = Decimal(level.new_quantity)  # absolute, never a delta
            if size > ZERO:
                side[price] = size
                continue
            if size < ZERO:  # impossible from a healthy exchange, so say so rather than store it
                self.negatives += 1
            side.pop(price, None)  # zero removes the level; the default covers one never held

    def best_bid(self) -> Decimal | None:
        """Return the highest price anyone is bidding.

        Reads the last key rather than scanning, because the side is kept in price order.
        Whether that was worth a sorted structure was settled by measurement, not taste.
        Replaying one fixed topic of 20,875 frames against a 22.6k by 21.0k book, the scans
        this method and best_ask used to do cost 1.75 ms of every frame, which was very
        nearly the whole cost of applying one. As end lookups the pair costs about 2 us, and
        applying a frame fell to 0.04 ms. The snapshot is the one thing that got slower:
        loading 43k levels one insert at a time went from 73 ms to 148 ms.

        Returns:
            The best bid, or None while that side is empty.
        """
        try:  # EAFP: an empty side raises here rather than handing back a default
            return self.bids.peekitem(-1)[0]  # last key, so the highest price on the side
        except IndexError:
            return None

    def best_ask(self) -> Decimal | None:
        """Return the lowest price anyone is offering.

        Returns:
            The best ask, or None while that side is empty.
        """
        try:  # EAFP: empty raises, exactly as it does for the bid side
            return self.asks.peekitem(0)[0]  # first key, so the lowest price on the side
        except IndexError:
            return None

    def spread(self) -> Decimal | None:
        """Return the gap between the best ask and the best bid.

        Returns:
            The spread, or None unless both sides have a level.
        """
        bid, ask = self.best_bid(), self.best_ask()
        if bid is None or ask is None:
            return None
        return ask - bid

    def check_crossed(self) -> bool:
        """Check that the best bid is still below the best ask, counting any breach.

        A crossed book means the two sides disagree, which is a lost or misapplied frame
        showing itself. It is counted rather than raised, because the run should report how
        often the book broke instead of stopping at the first sign.

        Returns:
            True when the book is crossed right now.
        """
        bid, ask = self.best_bid(), self.best_ask()
        if bid is None or ask is None:  # one empty side cannot cross the other
            return False
        if bid < ask:
            return False
        self.crossings += 1
        return True

    def quote(self) -> str:
        """Describe the top of the book in one short phrase.

        Returns:
            The best bid and ask with the depth behind each, for the closing summary
            and the --verbose line.
        """
        return f"bid={self.best_bid()}x{len(self.bids)} ask={self.best_ask()}x{len(self.asks)}"

    def status(self) -> str:
        """Summarise what the book holds and what went wrong getting there.

        Returns:
            Two lines: the last frame applied, then the top of the book and every fault
            counted during the run.
        """
        faults = f"crossings {self.crossings}, negatives {self.negatives}, skipped {self.skipped}"
        trust = " [doubtful: frames were missed]" if self.doubtful else ""
        return (
            f"[book] {self.frames} frames, {self.snapshots} snapshots, "
            f"last seq={self.sequence_num} at {self.timestamp}\n"
            f"[book] {self.quote()} spread={self.spread()}, {faults}{trust}"
        )
