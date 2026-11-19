"""Tests for SequenceWatch in src/book/main.py: spotting frames the exchange sent but we never saw.

Only SequenceWatch is tested here. The frames are built from the real decode structs with empty
events, because SequenceWatch reads nothing but each frame's sequence_num. Earlier frames are
fed in by `watch_after`, so every test makes exactly one call it is about.

Every test follows Arrange / Act / Assert, separated by blank lines.
"""

from collections.abc import Callable
from datetime import UTC, datetime

import pytest

from src.book.main import SequenceWatch
from src.decode.messages import (
    ErrorFrame,
    Frame,
    HeartbeatFrame,
    L2Frame,
    SubscriptionsFrame,
    UnknownFrame,
)

T0 = datetime(2026, 11, 18, 12, 0, 0, tzinfo=UTC)  # fixed; SequenceWatch never reads it anyway


# --- Builders -------------------------------------------------------------------------------


def l2(seq: int) -> L2Frame:
    """Build a level2 frame carrying only a counter.

    Args:
        seq: The frame's sequence_num.

    Returns:
        An L2Frame with no events.
    """
    return L2Frame(sequence_num=seq, timestamp=T0, events=[])


def heartbeat(seq: int) -> HeartbeatFrame:
    """Build a heartbeats frame carrying only a counter.

    Args:
        seq: The frame's sequence_num.

    Returns:
        A HeartbeatFrame with no ticks.
    """
    return HeartbeatFrame(sequence_num=seq, timestamp=T0, events=[])


def subscriptions(seq: int) -> SubscriptionsFrame:
    """Build a subscriptions acknowledgement carrying only a counter.

    Args:
        seq: The frame's sequence_num.

    Returns:
        A SubscriptionsFrame with no events.
    """
    return SubscriptionsFrame(sequence_num=seq, timestamp=T0, events=[])


def unknown(seq: int | None) -> UnknownFrame:
    """Build a frame from a channel the decoder does not model.

    Args:
        seq: The frame's sequence_num, or None when it carried none.

    Returns:
        An UnknownFrame from a made-up but realistic channel.
    """
    return UnknownFrame(channel="market_trades", sequence_num=seq)


def error() -> ErrorFrame:
    """Build an exchange error frame, the one frame type with no counter field at all.

    Returns:
        An ErrorFrame.
    """
    return ErrorFrame(message="rate limit exceeded")


def watch_after(*frames: Frame) -> SequenceWatch:
    """Build a watch that has already checked the given frames, in order.

    This is the Arrange step of a test. The loop lives here, so the tests stay straight-line.

    Args:
        *frames: The frames seen before the one the test is about.

    Returns:
        A SequenceWatch with those frames behind it.
    """
    watch = SequenceWatch()
    for frame in frames:
        watch.check(frame)
    return watch


# --- Constructor ----------------------------------------------------------------------------


def test_init_new_watch_has_no_starting_point() -> None:
    """None means "no counter seen yet", which lets the first frame set the starting point."""
    watch = SequenceWatch()

    assert watch.last is None


def test_init_new_watch_has_zero_counts() -> None:
    """Nothing has been checked, so nothing has been missed."""
    watch = SequenceWatch()

    assert (watch.checked, watch.gaps, watch.missed) == (0, 0, 0)


# --- The first frame ------------------------------------------------------------------------


@pytest.mark.parametrize("seq", [0, 1, 123456], ids=["zero", "one", "large"])
def test_check_first_frame_returns_empty_note(seq: int) -> None:
    """Whatever number the stream starts at, the first frame has nothing to be compared with."""
    watch = SequenceWatch()

    note = watch.check(l2(seq))

    assert note == ""


def test_check_first_frame_sets_starting_point() -> None:
    """The first counter becomes the number the next frame is compared with."""
    watch = SequenceWatch()

    watch.check(l2(7))

    assert (watch.last, watch.checked, watch.gaps) == (7, 1, 0)


# --- An unbroken run ------------------------------------------------------------------------


def test_check_next_counter_returns_empty_note() -> None:
    """A counter exactly one above the last is what a healthy stream looks like."""
    watch = watch_after(l2(5))

    note = watch.check(l2(6))

    assert note == ""


def test_check_unbroken_run_counts_every_frame_and_no_gaps() -> None:
    """Every frame with a counter is counted as checked; none of them is a gap."""
    watch = watch_after(l2(1), l2(2), l2(3))

    watch.check(l2(4))

    assert (watch.checked, watch.gaps, watch.missed) == (4, 0, 0)


# --- Gaps -----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("previous", "current", "expected"),
    [
        (5, 7, " [gap seq 5->7 missed 1]"),
        (5, 8, " [gap seq 5->8 missed 2]"),
        (0, 1000, " [gap seq 0->1000 missed 999]"),
    ],
    ids=["one-missed", "two-missed", "many-missed"],
)
def test_check_counter_jump_returns_gap_note(previous: int, current: int, expected: str) -> None:
    """A jump of more than one names both counters and how many frames fell between them."""
    watch = watch_after(l2(previous))

    note = watch.check(l2(current))

    assert note == expected


def test_check_counter_jump_counts_gap_and_missed_frames() -> None:
    """One jump is one gap; 5 to 8 means frames 6 and 7 never arrived."""
    watch = watch_after(l2(5))

    watch.check(l2(8))

    assert (watch.gaps, watch.missed) == (1, 2)


def test_check_counter_jump_still_counts_frame_as_checked() -> None:
    """The frame that reveals a gap arrived, so it is checked like any other."""
    watch = watch_after(l2(5))

    watch.check(l2(8))

    assert watch.checked == 2


def test_check_after_gap_next_counter_returns_empty_note() -> None:
    """After a jump, the new counter is the starting point, so the next frame is not a gap."""
    watch = watch_after(l2(5), l2(8))

    note = watch.check(l2(9))

    assert note == ""


def test_check_several_gaps_add_up() -> None:
    """Gaps and missed frames add up over the run: 1->3 misses 1, 3->6 misses 2."""
    watch = watch_after(l2(1), l2(3))

    watch.check(l2(6))

    assert (watch.gaps, watch.missed) == (2, 3)


# --- A counter that does not move forward ---------------------------------------------------


@pytest.mark.parametrize(
    ("previous", "current"),
    [(5, 5), (5, 4), (5, 0)],
    ids=["repeated", "one-lower", "reset-to-zero"],
)
def test_check_counter_not_ahead_returns_empty_note(previous: int, current: int) -> None:
    """The same or a lower counter is passed over in silence; Coinbase says it can be ignored."""
    watch = watch_after(l2(previous))

    note = watch.check(l2(current))

    assert note == ""


def test_check_counter_not_ahead_counts_no_gap() -> None:
    """A lower counter is checked, but it is not a gap and misses nothing."""
    watch = watch_after(l2(10))

    watch.check(l2(0))

    assert (watch.checked, watch.gaps, watch.missed) == (2, 0, 0)


def test_check_lower_counter_becomes_new_starting_point() -> None:
    """After a reconnect the exchange counts again from the start, so the new value is kept."""
    watch = watch_after(l2(10))

    watch.check(l2(0))

    assert watch.last == 0


def test_check_after_reset_continuing_run_returns_empty_note() -> None:
    """A restarted count that carries on by one is a healthy stream, not a gap."""
    watch = watch_after(l2(100), l2(0))

    note = watch.check(l2(1))

    assert note == ""


def test_check_after_reset_gap_is_measured_from_new_start() -> None:
    """After a restart, gaps are measured from the restarted count, not the old one."""
    watch = watch_after(l2(100), l2(0))

    note = watch.check(l2(3))

    assert note == " [gap seq 0->3 missed 2]"


def test_check_late_frame_then_next_frame_reports_extra_gap() -> None:
    """Current behaviour, pinned on purpose: a late frame leads to a gap that never happened.

    Frames 1, 2, 4, 3, 5: frame 3 arrives after 4. The watch cannot tell a late frame from a
    count restarted after a reconnect, so 3 becomes the starting point and 5 then looks like
    a jump over 4, even though 4 was already seen. This errs on the safe side: the book is
    marked doubtful too often, never too rarely. If this test fails, the behaviour was
    changed, and that should be a deliberate decision.
    """
    watch = watch_after(l2(1), l2(2), l2(4), l2(3))

    note = watch.check(l2(5))

    assert note == " [gap seq 3->5 missed 1]"


# --- Frames that carry no counter -----------------------------------------------------------


@pytest.mark.parametrize(
    "frame", [error(), unknown(None)], ids=["error-frame", "unknown-without-seq"]
)
def test_check_frame_without_counter_returns_empty_note(frame: Frame) -> None:
    """With no counter there is nothing to compare, so there is nothing to report."""
    watch = watch_after(l2(5))

    note = watch.check(frame)

    assert note == ""


@pytest.mark.parametrize(
    "frame", [error(), unknown(None)], ids=["error-frame", "unknown-without-seq"]
)
def test_check_frame_without_counter_leaves_watch_unchanged(frame: Frame) -> None:
    """A frame with no counter is not counted and does not move the starting point."""
    watch = watch_after(l2(5))

    watch.check(frame)

    assert (watch.last, watch.checked, watch.gaps, watch.missed) == (5, 1, 0, 0)


def test_check_frame_without_counter_between_frames_keeps_run_unbroken() -> None:
    """An error frame between 1 and 2 does not break the run."""
    watch = watch_after(l2(1), error())

    note = watch.check(l2(2))

    assert note == ""


def test_check_frame_without_counter_first_leaves_no_starting_point() -> None:
    """If the very first frame has no counter, the next frame with one still sets the start."""
    watch = SequenceWatch()

    watch.check(error())

    assert watch.last is None


# --- Every channel shares one counter -------------------------------------------------------


@pytest.mark.parametrize(
    "make",
    [l2, heartbeat, subscriptions, unknown],
    ids=["level2", "heartbeats", "subscriptions", "unknown-with-seq"],
)
def test_check_any_channel_with_counter_reports_gap(make: Callable[[int], Frame]) -> None:
    """Every frame type that carries a counter is checked the same way."""
    watch = watch_after(make(1))

    note = watch.check(make(3))

    assert note == " [gap seq 1->3 missed 1]"


def test_check_mixed_channels_in_order_returns_empty_note() -> None:
    """One connection numbers all its frames together, whatever channel each came from."""
    watch = watch_after(l2(1), heartbeat(2), subscriptions(3))

    note = watch.check(l2(4))

    assert note == ""


def test_check_lost_frame_during_quiet_book_shows_through_heartbeat() -> None:
    """A frame lost while the book is quiet still shows up, through the heartbeats.

    A heartbeat lands about once a second, so the counter keeps moving even when the book does
    not. This is why the heartbeats channel is subscribed.
    """
    watch = watch_after(l2(1), heartbeat(2))

    note = watch.check(heartbeat(4))

    assert note == " [gap seq 2->4 missed 1]"


# --- report ---------------------------------------------------------------------------------


def test_report_new_watch_shows_zero_counts() -> None:
    """A run that saw nothing says so plainly."""
    watch = SequenceWatch()

    line = watch.report()

    assert line == "[stream] checked 0 frames, 0 gaps (0 missed)"


def test_report_after_gaps_shows_checked_gaps_and_missed() -> None:
    """Frames 1, 3, 6, 7: four checked, two gaps (1->3 and 3->6), three frames missed."""
    watch = watch_after(l2(1), l2(3), l2(6), l2(7))

    line = watch.report()

    assert line == "[stream] checked 4 frames, 2 gaps (3 missed)"
