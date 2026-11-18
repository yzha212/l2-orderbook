"""Tests for src/book/book.py: the in-memory Level 2 order book and the record it publishes.

The frames fed to the book are built straight from the real decode structs (L2Frame, Snapshot,
Update, PriceLevel). They are plain value objects, so using them is more realistic than a mock
and does not test the decode stage itself. Tests of the read-only methods fill the public
`bids` / `asks` sides directly with `book_with`, so they do not depend on `apply` working.

Every test follows Arrange / Act / Assert, separated by blank lines.
"""

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import msgspec
import pytest
from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule

from src.book.book import OrderBook, TopLevels
from src.decode.messages import L2Frame, PriceLevel, Snapshot, Update

T0 = datetime(2026, 11, 18, 12, 0, 0, tzinfo=UTC)  # fixed, so no test depends on the clock
T1 = T0 + timedelta(seconds=1)
EVENT_TIME = "2026-11-18T12:00:00.000000Z"  # the book never reads it, but the struct needs one
PRODUCT = "BTC-USD"


# --- Builders -------------------------------------------------------------------------------


def bid(price: str, qty: str) -> PriceLevel:
    """Build one bid-side level, with price and size as text, exactly as the exchange sends them.

    Args:
        price: The price level, e.g. "100.00".
        qty: The new absolute size at that price; "0" removes the level.

    Returns:
        A bid-side PriceLevel.
    """
    return PriceLevel(side="bid", event_time=EVENT_TIME, price_level=price, new_quantity=qty)


def ask(price: str, qty: str) -> PriceLevel:
    """Build one ask-side level. The exchange calls this side "offer".

    Args:
        price: The price level, e.g. "101.00".
        qty: The new absolute size at that price; "0" removes the level.

    Returns:
        An offer-side PriceLevel.
    """
    return PriceLevel(side="offer", event_time=EVENT_TIME, price_level=price, new_quantity=qty)


def snapshot(*levels: PriceLevel) -> Snapshot:
    """Build a snapshot event carrying the given levels.

    Args:
        *levels: Every level of the new book.

    Returns:
        A Snapshot event.
    """
    return Snapshot(product_id=PRODUCT, updates=list(levels))


def update(*levels: PriceLevel) -> Update:
    """Build an update event carrying the given levels.

    Args:
        *levels: The levels that changed.

    Returns:
        An Update event.
    """
    return Update(product_id=PRODUCT, updates=list(levels))


def frame(*events: Snapshot | Update, seq: int = 1, ts: datetime = T0) -> L2Frame:
    """Wrap events in a level2 frame.

    Args:
        *events: The snapshot and update events, in order.
        seq: The frame's sequence number.
        ts: When the exchange sent the frame.

    Returns:
        An L2Frame ready for OrderBook.apply.
    """
    return L2Frame(sequence_num=seq, timestamp=ts, events=list(events))


def decimals(*pairs: tuple[str, str]) -> list[tuple[Decimal, Decimal]]:
    """Turn (price, size) text pairs into the Decimal pairs the book holds.

    Args:
        *pairs: (price, size) pairs as text.

    Returns:
        The same pairs as Decimals, in the order given.
    """
    return [(Decimal(price), Decimal(size)) for price, size in pairs]


def book_with(
    bids: Iterable[tuple[str, str]] = (), asks: Iterable[tuple[str, str]] = ()
) -> OrderBook:
    """Build a book by filling its public sides directly, without going through apply.

    Args:
        bids: (price, size) text pairs for the bid side.
        asks: (price, size) text pairs for the ask side.

    Returns:
        A book holding exactly those levels, with every counter still at zero.
    """
    book = OrderBook()
    book.bids.update(decimals(*bids))
    book.asks.update(decimals(*asks))
    return book


# --- Fixtures -------------------------------------------------------------------------------
# Function scope (the pytest default): every test gets a brand-new book, so no test can see
# another test's levels. The book lives only in memory, so there is nothing to tear down.


@pytest.fixture
def book() -> OrderBook:
    """Return an empty book that has loaded nothing.

    Returns:
        A new OrderBook.
    """
    return OrderBook()


@pytest.fixture
def loaded_book(book: OrderBook) -> OrderBook:
    """Return a book loaded from one snapshot (frame seq=1, sent at T0).

    Bids: 100.00 x 2 (best), 99.00 x 1. Asks: 101.00 x 3 (best), 102.00 x 4.

    Args:
        book: An empty book, from the fixture above.

    Returns:
        The same book, now loaded.
    """
    levels = (bid("99.00", "1"), bid("100.00", "2"), ask("101.00", "3"), ask("102.00", "4"))
    book.apply(frame(snapshot(*levels)))
    return book


# --- Constructor and `loaded` ---------------------------------------------------------------


def test_init_new_book_has_no_levels() -> None:
    """A new book holds nothing on either side."""
    new_book = OrderBook()

    assert len(new_book.bids) == 0
    assert len(new_book.asks) == 0


def test_init_new_book_has_no_frame_applied() -> None:
    """None is what "no frame applied yet" means for the timestamp and sequence number."""
    new_book = OrderBook()

    assert new_book.timestamp is None
    assert new_book.sequence_num is None
    assert new_book.frames == 0


def test_init_new_book_has_no_faults_counted() -> None:
    """Every fault counter starts at zero and the book is not marked doubtful."""
    new_book = OrderBook()

    assert (new_book.snapshots, new_book.skipped, new_book.crossings, new_book.negatives) == (
        0,
        0,
        0,
        0,
    )
    assert new_book.doubtful is False


def test_loaded_new_book_returns_false(book: OrderBook) -> None:
    """A book that never saw a snapshot is not loaded."""
    assert book.loaded is False


def test_loaded_after_snapshot_returns_true(book: OrderBook) -> None:
    """One snapshot is enough to make updates meaningful."""
    book.apply(frame(snapshot(bid("100.00", "1"))))

    assert book.loaded is True


def test_loaded_after_updates_only_returns_false(book: OrderBook) -> None:
    """Updates alone never count as loading the book."""
    book.apply(frame(update(bid("100.00", "1"))))

    assert book.loaded is False


# --- Loading a snapshot ---------------------------------------------------------------------


def test_apply_snapshot_on_empty_book_loads_both_sides(book: OrderBook) -> None:
    """Bids go to the bid side and offers go to the ask side."""
    book.apply(frame(snapshot(bid("100.00", "2"), ask("101.00", "3"))))

    assert book.bids.items()[:] == decimals(("100.00", "2"))
    assert book.asks.items()[:] == decimals(("101.00", "3"))


def test_apply_snapshot_offer_level_lands_on_ask_side_only(book: OrderBook) -> None:
    """The exchange's "offer" is the book's ask; it must never reach the bid side."""
    book.apply(frame(snapshot(ask("101.00", "3"))))

    assert list(book.asks) == [Decimal("101.00")]
    assert len(book.bids) == 0


def test_apply_snapshot_text_becomes_exact_decimal(book: OrderBook) -> None:
    """Prices and sizes keep the exchange's exact text, trailing zeros included."""
    book.apply(frame(snapshot(bid("111234.57", "0.00500000"))))

    [(price, size)] = book.bids.items()
    assert type(price) is Decimal
    assert type(size) is Decimal
    assert (str(price), str(size)) == ("111234.57", "0.00500000")


def test_apply_snapshot_prices_sort_as_numbers_not_text(book: OrderBook) -> None:
    """As text "9.50" sorts above "10.50"; as numbers it must not."""
    levels = (bid("9.50", "1"), bid("10.50", "1"), ask("9.50", "1"), ask("10.50", "1"))

    book.apply(frame(snapshot(*levels)))

    assert book.best_bid() == Decimal("10.50")
    assert book.best_ask() == Decimal("9.50")


def test_apply_snapshot_counts_one_snapshot(book: OrderBook) -> None:
    """Each snapshot loaded adds one to the snapshot counter."""
    book.apply(frame(snapshot(bid("100.00", "1"))))

    assert book.snapshots == 1


def test_apply_second_snapshot_replaces_whole_book(loaded_book: OrderBook) -> None:
    """A snapshot replaces the book rather than adding to it: old levels are gone."""
    loaded_book.apply(frame(snapshot(bid("50.00", "7"), ask("60.00", "8")), seq=2))

    assert loaded_book.bids.items()[:] == decimals(("50.00", "7"))
    assert loaded_book.asks.items()[:] == decimals(("60.00", "8"))


def test_apply_empty_snapshot_empties_book_and_keeps_it_loaded(loaded_book: OrderBook) -> None:
    """An empty snapshot means the exchange holds nothing, which is still a loaded book."""
    loaded_book.apply(frame(snapshot(), seq=2))

    assert len(loaded_book.bids) == 0
    assert len(loaded_book.asks) == 0
    assert loaded_book.loaded is True


def test_apply_snapshot_zero_size_level_is_not_stored(book: OrderBook) -> None:
    """A size of zero never rests in the book, even inside a snapshot."""
    book.apply(frame(snapshot(bid("100.00", "0"), bid("99.00", "1"))))

    assert list(book.bids) == [Decimal("99.00")]


# --- Applying incremental updates -----------------------------------------------------------


def test_apply_update_before_snapshot_leaves_book_empty(book: OrderBook) -> None:
    """Edits to a book never loaded would invent one, so they are dropped."""
    book.apply(frame(update(bid("100.00", "1"), ask("101.00", "1"))))

    assert len(book.bids) == 0
    assert len(book.asks) == 0


def test_apply_update_before_snapshot_counts_each_event_skipped(book: OrderBook) -> None:
    """The skip counter goes up once per update event, not once per frame."""
    book.apply(frame(update(bid("100.00", "1")), update(ask("101.00", "1"))))

    assert book.skipped == 2


def test_apply_update_new_price_adds_level(loaded_book: OrderBook) -> None:
    """A price the book does not hold yet becomes a new level."""
    loaded_book.apply(frame(update(bid("98.00", "5")), seq=2))

    assert loaded_book.bids.items()[:] == decimals(("98.00", "5"), ("99.00", "1"), ("100.00", "2"))


def test_apply_update_existing_price_replaces_size(loaded_book: OrderBook) -> None:
    """Sizes are absolute, never deltas: 2 then 0.5 means 0.5, not 2.5."""
    loaded_book.apply(frame(update(bid("100.00", "0.5")), seq=2))

    assert loaded_book.bids[Decimal("100.00")] == Decimal("0.5")


def test_apply_update_same_price_written_differently_edits_same_level(
    loaded_book: OrderBook,
) -> None:
    """The texts "100.0" and "100.00" are one price, so they must be one level."""
    loaded_book.apply(frame(update(bid("100.0", "9")), seq=2))

    assert len(loaded_book.bids) == 2
    assert loaded_book.bids[Decimal("100.00")] == Decimal("9")


def test_apply_update_offer_level_edits_ask_side(loaded_book: OrderBook) -> None:
    """An offer-side update changes the ask side and leaves the bids alone."""
    loaded_book.apply(frame(update(ask("101.00", "7")), seq=2))

    assert loaded_book.asks[Decimal("101.00")] == Decimal("7")
    assert loaded_book.bids.items()[:] == decimals(("99.00", "1"), ("100.00", "2"))


def test_apply_snapshot_then_update_in_one_frame_applies_both(book: OrderBook) -> None:
    """Events in one frame land in order, so the update sees the snapshot before it."""
    book.apply(frame(snapshot(bid("100.00", "1")), update(bid("99.00", "2"))))

    assert book.bids.items()[:] == decimals(("99.00", "2"), ("100.00", "1"))
    assert book.skipped == 0


# --- Size 0 removes a price level -----------------------------------------------------------


@pytest.mark.parametrize("zero", ["0", "0.00000000", "0E-8"], ids=["plain", "padded", "exponent"])
def test_apply_update_zero_size_removes_level(loaded_book: OrderBook, zero: str) -> None:
    """Every spelling of zero removes the level, because the size is compared as a number."""
    loaded_book.apply(frame(update(bid("99.00", zero)), seq=2))

    assert Decimal("99.00") not in loaded_book.bids


def test_apply_update_zero_size_for_unknown_price_leaves_book_unchanged(
    loaded_book: OrderBook,
) -> None:
    """Removing a level the book never held is harmless and raises nothing."""
    bids_before, asks_before = dict(loaded_book.bids), dict(loaded_book.asks)

    loaded_book.apply(frame(update(bid("42.00", "0"), ask("4200.00", "0")), seq=2))

    assert dict(loaded_book.bids) == bids_before
    assert dict(loaded_book.asks) == asks_before


def test_apply_update_zero_size_on_best_bid_makes_next_price_best(loaded_book: OrderBook) -> None:
    """Removing the top level hands the best price to the next one down."""
    loaded_book.apply(frame(update(bid("100.00", "0")), seq=2))

    assert loaded_book.best_bid() == Decimal("99.00")


def test_apply_update_zero_size_on_last_level_empties_side(book: OrderBook) -> None:
    """Removing the only level on a side leaves that side empty."""
    book.apply(frame(snapshot(ask("101.00", "3"))))

    book.apply(frame(update(ask("101.00", "0")), seq=2))

    assert len(book.asks) == 0


# --- Negative sizes -------------------------------------------------------------------------


def test_apply_update_negative_size_is_counted(loaded_book: OrderBook) -> None:
    """A size below zero is impossible from a healthy exchange, so it is counted."""
    loaded_book.apply(frame(update(bid("98.00", "-1")), seq=2))

    assert loaded_book.negatives == 1


def test_apply_update_negative_size_is_not_stored(loaded_book: OrderBook) -> None:
    """A negative size never rests in the book."""
    loaded_book.apply(frame(update(bid("98.00", "-1")), seq=2))

    assert Decimal("98.00") not in loaded_book.bids


def test_apply_update_negative_size_on_held_price_removes_level(loaded_book: OrderBook) -> None:
    """Current behaviour: a negative size removes the level, the same way zero does."""
    loaded_book.apply(frame(update(bid("100.00", "-1")), seq=2))

    assert Decimal("100.00") not in loaded_book.bids


# --- Frame bookkeeping ----------------------------------------------------------------------


def test_apply_frame_records_its_timestamp_and_sequence_num(book: OrderBook) -> None:
    """The book is stamped with the frame it is complete as of."""
    book.apply(frame(snapshot(bid("100.00", "1")), seq=7, ts=T1))

    assert book.timestamp == T1
    assert book.sequence_num == 7


def test_apply_later_frame_overwrites_timestamp_and_sequence_num(loaded_book: OrderBook) -> None:
    """Only the most recent frame applied is kept."""
    loaded_book.apply(frame(update(bid("98.00", "1")), seq=2, ts=T1))

    assert loaded_book.timestamp == T1
    assert loaded_book.sequence_num == 2


def test_apply_each_frame_is_counted(loaded_book: OrderBook) -> None:
    """The frame counter goes up once per frame applied."""
    loaded_book.apply(frame(update(bid("98.00", "1")), seq=2))

    assert loaded_book.frames == 2


def test_apply_frame_with_no_events_still_records_frame(book: OrderBook) -> None:
    """A frame with nothing in it is still a frame the book has seen."""
    book.apply(frame(seq=5, ts=T1))

    assert (book.frames, book.sequence_num, book.timestamp) == (1, 5, T1)


def test_apply_frame_with_only_skipped_updates_still_records_frame(book: OrderBook) -> None:
    """Skipping the events does not mean skipping the frame's bookkeeping."""
    book.apply(frame(update(bid("100.00", "1")), seq=3, ts=T1))

    assert (book.frames, book.sequence_num, book.timestamp) == (1, 3, T1)


# --- Best bid, best ask and the spread ------------------------------------------------------


def test_best_bid_empty_side_returns_none() -> None:
    """With no bids there is no best bid."""
    empty_bids = book_with(asks=[("101", "1")])

    assert empty_bids.best_bid() is None


def test_best_ask_empty_side_returns_none() -> None:
    """With no asks there is no best ask."""
    empty_asks = book_with(bids=[("100", "1")])

    assert empty_asks.best_ask() is None


def test_best_bid_several_levels_returns_highest_price() -> None:
    """The best bid is the highest price anyone will pay."""
    several = book_with(bids=[("99", "1"), ("101", "1"), ("100", "1")])

    assert several.best_bid() == Decimal("101")


def test_best_ask_several_levels_returns_lowest_price() -> None:
    """The best ask is the lowest price anyone will sell at."""
    several = book_with(asks=[("103", "1"), ("101", "1"), ("102", "1")])

    assert several.best_ask() == Decimal("101")


def test_spread_both_sides_returns_exact_difference() -> None:
    """Decimal keeps the spread exact; as floats 100.01 - 100.00 is 0.010000000000005116."""
    one_cent = book_with(bids=[("100.00", "1")], asks=[("100.01", "1")])

    assert one_cent.spread() == Decimal("0.01")


@pytest.mark.parametrize(
    ("bids", "asks"),
    [([("100", "1")], []), ([], [("101", "1")]), ([], [])],
    ids=["no-asks", "no-bids", "empty"],
)
def test_spread_a_side_empty_returns_none(
    bids: list[tuple[str, str]], asks: list[tuple[str, str]]
) -> None:
    """A spread needs a price on both sides."""
    one_sided = book_with(bids=bids, asks=asks)

    assert one_sided.spread() is None


def test_spread_crossed_book_returns_negative() -> None:
    """Current behaviour: a crossed book gives a negative spread rather than None."""
    crossed = book_with(bids=[("101", "1")], asks=[("100", "1")])

    assert crossed.spread() == Decimal("-1")


# --- The crossed-book check -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("best_bid", "best_ask", "expected"),
    [("100.00", "100.01", False), ("100.00", "100.00", True), ("100.02", "100.01", True)],
    ids=["bid-below-ask", "locked-bid-equals-ask", "bid-above-ask"],
)
def test_check_crossed_bid_against_ask_returns_expected(
    best_bid: str, best_ask: str, expected: bool
) -> None:
    """The book is broken when the best bid is at or above the best ask (locked counts)."""
    two_sided = book_with(bids=[(best_bid, "1")], asks=[(best_ask, "1")])

    assert two_sided.check_crossed() is expected


@pytest.mark.parametrize(
    ("bids", "asks"),
    [([("100", "1")], []), ([], [("101", "1")]), ([], [])],
    ids=["no-asks", "no-bids", "empty"],
)
def test_check_crossed_a_side_empty_returns_false(
    bids: list[tuple[str, str]], asks: list[tuple[str, str]]
) -> None:
    """One empty side cannot cross the other."""
    one_sided = book_with(bids=bids, asks=asks)

    assert one_sided.check_crossed() is False


def test_check_crossed_crossed_book_counts_every_call() -> None:
    """Each check that finds the book crossed adds one, so the run reports how often."""
    crossed = book_with(bids=[("101", "1")], asks=[("100", "1")])

    crossed.check_crossed()
    crossed.check_crossed()

    assert crossed.crossings == 2


def test_check_crossed_sound_book_leaves_count_unchanged() -> None:
    """A sound book adds nothing to the crossing counter."""
    sound = book_with(bids=[("100", "1")], asks=[("101", "1")])

    sound.check_crossed()

    assert sound.crossings == 0


def test_apply_sound_frame_counts_no_crossing(loaded_book: OrderBook) -> None:
    """apply runs the check itself; a sound book stays at zero."""
    loaded_book.apply(frame(update(bid("100.50", "1")), seq=2))

    assert loaded_book.crossings == 0


def test_apply_update_that_crosses_book_counts_crossing(loaded_book: OrderBook) -> None:
    """apply runs the check after every frame, so a crossing update is counted."""
    loaded_book.apply(frame(update(bid("101.00", "1")), seq=2))  # bid now equals the best ask

    assert loaded_book.crossings == 1


def test_apply_update_that_crosses_book_still_applies_levels(loaded_book: OrderBook) -> None:
    """A crossing is counted, not raised, and the update is not rolled back."""
    loaded_book.apply(frame(update(bid("101.50", "1")), seq=2))

    assert loaded_book.best_bid() == Decimal("101.50")


# --- The top N levels -----------------------------------------------------------------------


def test_top_returns_best_price_first_on_each_side() -> None:
    """Bids come highest first and asks lowest first: both start at the best price."""
    deep = book_with(
        bids=[("98", "2"), ("99", "3"), ("100", "4")],
        asks=[("101", "5"), ("102", "6"), ("103", "7")],
    )

    levels = deep.top(3)

    assert levels.bids == decimals(("100", "4"), ("99", "3"), ("98", "2"))
    assert levels.asks == decimals(("101", "5"), ("102", "6"), ("103", "7"))


def test_top_depth_smaller_than_side_returns_only_that_many() -> None:
    """The depth caps how many levels come from each side."""
    deep = book_with(
        bids=[("98", "2"), ("99", "3"), ("100", "4")],
        asks=[("101", "5"), ("102", "6"), ("103", "7")],
    )

    levels = deep.top(2)

    assert levels.bids == decimals(("100", "4"), ("99", "3"))
    assert levels.asks == decimals(("101", "5"), ("102", "6"))


def test_top_depth_larger_than_side_returns_whole_side() -> None:
    """A side holding fewer levels than asked gives up everything it has."""
    shallow = book_with(bids=[("99", "1")], asks=[("101", "1"), ("102", "2")])

    levels = shallow.top(10)

    assert levels.bids == decimals(("99", "1"))
    assert levels.asks == decimals(("101", "1"), ("102", "2"))


def test_top_depth_one_returns_best_level_only() -> None:
    """Depth one, the smallest allowed, is just the best price on each side."""
    deep = book_with(bids=[("99", "1"), ("100", "2")], asks=[("101", "3"), ("102", "4")])

    levels = deep.top(1)

    assert levels.bids == decimals(("100", "2"))
    assert levels.asks == decimals(("101", "3"))


def test_top_empty_book_returns_empty_sides(book: OrderBook) -> None:
    """An empty book publishes empty sides rather than failing."""
    levels = book.top(10)

    assert levels.bids == []
    assert levels.asks == []


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="known bug: top() does not guard depth < 1, and bids.items()[-0:] is the whole side",
)
@pytest.mark.parametrize("depth", [0, -1], ids=["zero", "negative"])
def test_top_depth_below_one_returns_no_levels(depth: int) -> None:
    """Asking for fewer than one level should never hand back levels.

    Marked xfail(strict=True): it fails today because of the bug above. Once top() is fixed
    this test passes, strict turns that into a failed run, and the mark must be removed.
    """
    two_sided = book_with(bids=[("99", "1"), ("100", "2")], asks=[("101", "3")])

    levels = two_sided.top(depth)

    assert levels.bids == []
    assert levels.asks == []


def test_top_record_carries_last_frame_and_doubtful_flag(loaded_book: OrderBook) -> None:
    """The record says which frame the book is complete as of, and whether to trust it."""
    loaded_book.doubtful = True

    levels = loaded_book.top(1)

    assert (levels.sequence_num, levels.timestamp, levels.doubtful) == (1, T0, True)


def test_top_published_at_is_current_utc_time(loaded_book: OrderBook) -> None:
    """published_at is when the record was built, in UTC.

    Checked against a before/after window instead of freezing the clock.
    """
    before = datetime.now(UTC)

    levels = loaded_book.top(1)

    after = datetime.now(UTC)
    assert before <= levels.published_at <= after
    assert levels.published_at.utcoffset() == timedelta(0)


def test_top_leaves_book_unchanged(loaded_book: OrderBook) -> None:
    """Taking the top is a read: the book and its counters stay as they were."""
    bids_before, asks_before = dict(loaded_book.bids), dict(loaded_book.asks)

    loaded_book.top(1)

    assert dict(loaded_book.bids) == bids_before
    assert dict(loaded_book.asks) == asks_before
    assert loaded_book.crossings == 0


# --- The published TopLevels record ---------------------------------------------------------


def test_toplevels_json_round_trip_returns_equal_record() -> None:
    """What the book builder writes is exactly what the dashboard and archiver read back."""
    record = TopLevels(
        sequence_num=7,
        timestamp=T0,
        published_at=T1,
        doubtful=False,
        bids=decimals(("100.00", "0.00500000")),
        asks=decimals(("101.00", "2")),
    )

    decoded = msgspec.json.decode(msgspec.json.encode(record), type=TopLevels)

    assert decoded == record


def test_toplevels_json_round_trip_with_no_frame_keeps_nones() -> None:
    """A record from a book that has applied nothing yet survives the trip too."""
    record = TopLevels(
        sequence_num=None, timestamp=None, published_at=T1, doubtful=False, bids=[], asks=[]
    )

    decoded = msgspec.json.decode(msgspec.json.encode(record), type=TopLevels)

    assert decoded == record


def test_toplevels_json_writes_prices_and_sizes_as_exact_strings() -> None:
    """JSON numbers would arrive as floats and lose "0.00500000"; strings keep it exact."""
    record = TopLevels(
        sequence_num=7,
        timestamp=T0,
        published_at=T1,
        doubtful=False,
        bids=decimals(("100.00", "0.00500000")),
        asks=[],
    )

    payload = msgspec.json.decode(msgspec.json.encode(record))  # untyped: plain Python values

    assert payload["bids"] == [["100.00", "0.00500000"]]


# --- quote and status -----------------------------------------------------------------------


def test_quote_returns_best_prices_and_level_counts() -> None:
    """The quote is best price x number of levels, for each side."""
    two_sided = book_with(bids=[("99", "1"), ("100", "2")], asks=[("101", "3")])

    assert two_sided.quote() == "bid=100x2 ask=101x1"


def test_quote_empty_book_shows_none_and_zero(book: OrderBook) -> None:
    """An empty book still produces a quote line instead of failing."""
    assert book.quote() == "bid=Nonex0 ask=Nonex0"


def test_status_first_line_reports_frames_and_last_frame(loaded_book: OrderBook) -> None:
    """Line one: how many frames and snapshots, and which frame came last."""
    first_line = loaded_book.status().splitlines()[0]

    assert first_line == f"[book] 1 frames, 1 snapshots, last seq=1 at {T0}"


def test_status_second_line_reports_top_spread_and_faults(loaded_book: OrderBook) -> None:
    """Line two: the quote, the spread, and every fault counted."""
    second_line = loaded_book.status().splitlines()[1]

    assert second_line == (
        "[book] bid=100.00x2 ask=101.00x2 spread=1.00, crossings 0, negatives 0, skipped 0"
    )


def test_status_doubtful_book_adds_warning(loaded_book: OrderBook) -> None:
    """A book that may have drifted says so at the end of the status."""
    loaded_book.doubtful = True

    status = loaded_book.status()

    assert status.endswith(" [doubtful: frames were missed]")


def test_status_sound_book_has_no_warning(loaded_book: OrderBook) -> None:
    """A sound book carries no doubt warning."""
    status = loaded_book.status()

    assert "doubtful" not in status


# --- Model-based test (Hypothesis) ----------------------------------------------------------
# Hypothesis plays random sequences of snapshot and update frames into a real OrderBook and
# into a deliberately plain model (two dicts), then checks after every step that the two agree.
# Prices come from a narrow grid so the same price is added, replaced and removed often.

PRICES = st.integers(min_value=9990, max_value=10010).map(  # 99.90 to 100.10, no floats
    lambda cents: str(Decimal(cents).scaleb(-2))
)
SIZES = st.sampled_from(["0", "0.00000000", "-1", "0.5", "1", "2.25000000"])
LEVELS = st.lists(
    st.builds(
        PriceLevel,
        side=st.sampled_from(["bid", "offer"]),
        event_time=st.just(EVENT_TIME),
        price_level=PRICES,
        new_quantity=SIZES,
    ),
    max_size=8,
)


class OrderBookMachine(RuleBasedStateMachine):
    """Drive an OrderBook and a plain dict model with the same frames and compare them."""

    def __init__(self) -> None:
        """Start both the real book and the model empty."""
        super().__init__()
        self.book = OrderBook()
        self.model: dict[str, dict[Decimal, Decimal]] = {"bid": {}, "offer": {}}
        self.loaded = False
        self.seq = 0
        self.skipped = 0
        self.negatives = 0
        self.crossings = 0

    def _send(self, event: Snapshot | Update) -> None:
        """Apply one event to the real book, in a frame of its own."""
        self.seq += 1
        self.book.apply(frame(event, seq=self.seq))

    def _model_load(self, levels: list[PriceLevel]) -> None:
        """Apply levels to the model: a positive size sets, anything else removes."""
        for level in levels:
            side = self.model[level.side]
            price, size = Decimal(level.price_level), Decimal(level.new_quantity)
            if size > 0:
                side[price] = size
            else:
                side.pop(price, None)
                self.negatives += size < 0

    def _model_count_crossing(self) -> None:
        """Count a crossing the same way the book should: best bid at or above best ask."""
        bids, asks = self.model["bid"], self.model["offer"]
        if bids and asks and max(bids) >= min(asks):
            self.crossings += 1

    @rule(levels=LEVELS)
    def load_snapshot(self, levels: list[PriceLevel]) -> None:
        """Send a snapshot: the model is cleared and reloaded."""
        self._send(snapshot(*levels))
        self.model = {"bid": {}, "offer": {}}
        self.loaded = True
        self._model_load(levels)
        self._model_count_crossing()

    @rule(levels=LEVELS)
    def apply_update(self, levels: list[PriceLevel]) -> None:
        """Send an update: applied once loaded, otherwise counted as skipped."""
        self._send(update(*levels))
        if self.loaded:
            self._model_load(levels)
        else:
            self.skipped += 1
        self._model_count_crossing()

    @invariant()
    def sides_match_model(self) -> None:
        """Both sides hold exactly the model's levels."""
        assert dict(self.book.bids) == self.model["bid"]
        assert dict(self.book.asks) == self.model["offer"]

    @invariant()
    def no_level_rests_with_size_zero_or_below(self) -> None:
        """The architecture's guarantee: sizes in the book are never zero or negative."""
        assert all(size > 0 for size in self.book.bids.values())
        assert all(size > 0 for size in self.book.asks.values())

    @invariant()
    def best_prices_and_spread_match_model(self) -> None:
        """Best bid, best ask and spread agree with a plain max / min over the model."""
        best_bid = max(self.model["bid"], default=None)
        best_ask = min(self.model["offer"], default=None)
        both = best_bid is not None and best_ask is not None
        assert self.book.best_bid() == best_bid
        assert self.book.best_ask() == best_ask
        assert self.book.spread() == (best_ask - best_bid if both else None)

    @invariant()
    def top_levels_match_model(self) -> None:
        """top(3) is the model's three best levels on each side, best first."""
        levels = self.book.top(3)
        assert levels.bids == sorted(self.model["bid"].items(), reverse=True)[:3]
        assert levels.asks == sorted(self.model["offer"].items())[:3]

    @invariant()
    def counters_match_model(self) -> None:
        """Frames, skips, negatives, crossings and the loaded flag all agree."""
        book = self.book
        assert (book.frames, book.skipped, book.negatives, book.crossings, book.loaded) == (
            self.seq,
            self.skipped,
            self.negatives,
            self.crossings,
            self.loaded,
        )


# deadline=None: no time limit per example, so a slow machine or container cannot make it flaky.
OrderBookMachine.TestCase.settings = settings(
    max_examples=100, stateful_step_count=30, deadline=None
)
TestOrderBookMatchesDictModel = OrderBookMachine.TestCase
