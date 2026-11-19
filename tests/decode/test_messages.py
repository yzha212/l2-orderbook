"""Tests for decode_frame in src/decode/messages.py: raw exchange bytes in, typed frames out.

The frames below are real. They were copied by hand from a capture and cut down to a few levels
each, then kept as Python dicts so a test can change one field without editing JSON text.
`encode` turns a dict back into bytes laid out exactly as the exchange sends them. The capture
folder itself (raw/) is never read: it is git-ignored and gets regenerated.

Every test follows Arrange / Act / Assert, separated by blank lines.
"""

import json
from datetime import UTC, datetime, timedelta

import msgspec
import pytest

from src.decode.messages import (
    ErrorFrame,
    Heartbeat,
    HeartbeatFrame,
    L2Frame,
    PriceLevel,
    Snapshot,
    Subscriptions,
    SubscriptionsFrame,
    UnknownFrame,
    Update,
    decode_frame,
)

# --- Real frames ----------------------------------------------------------------------------
# From raw/symbol=BTC-USD/date=2026-09-21/20260921T0900.ndjson.zst, copied by hand. The
# snapshot held 40,306 levels and the update 28; a few of each are kept here. Keys stay in the
# exchange's order. These constants are never changed in place: a test that needs a variation
# builds a new dict from them with {**CONSTANT, "key": new_value}.

SNAPSHOT_BID = {
    "side": "bid",
    "event_time": "2026-09-21T09:13:18.719338Z",
    "price_level": "83979.06",
    "new_quantity": "0.02912259",
}
SNAPSHOT_OFFER = {
    "side": "offer",
    "event_time": "2026-09-21T09:13:18.719338Z",
    "price_level": "83979.07",
    "new_quantity": "0.00015481",
}
SNAPSHOT_EVENT = {
    "type": "snapshot",
    "product_id": "BTC-USD",
    "updates": [SNAPSHOT_BID, SNAPSHOT_OFFER],
}
SNAPSHOT_FRAME = {  # offset 1: the first l2_data frame of the connection
    "channel": "l2_data",
    "timestamp": "2026-09-21T09:13:19.03548968Z",
    "sequence_num": 1,
    "events": [SNAPSHOT_EVENT],
}

UPDATE_BID = {
    "side": "bid",
    "event_time": "2026-09-21T09:13:18.842328Z",
    "price_level": "83979.06",
    "new_quantity": "0.02889672",
}
UPDATE_REMOVAL = {  # a size of "0" removes the level
    "side": "bid",
    "event_time": "2026-09-21T09:13:18.842328Z",
    "price_level": "83955.05",
    "new_quantity": "0",
}
UPDATE_OFFER = {
    "side": "offer",
    "event_time": "2026-09-21T09:13:18.842328Z",
    "price_level": "83982.66",
    "new_quantity": "0.02380475",
}
UPDATE_EVENT = {
    "type": "update",
    "product_id": "BTC-USD",
    "updates": [UPDATE_BID, UPDATE_REMOVAL, UPDATE_OFFER],
}
UPDATE_FRAME = {  # offset 2: nine fraction digits in the timestamp, as the exchange sends
    "channel": "l2_data",
    "timestamp": "2026-09-21T09:13:19.054277336Z",
    "sequence_num": 2,
    "events": [UPDATE_EVENT],
}

HEARTBEAT = {  # current_time is Go's own time format, not ISO 8601
    "current_time": "2026-09-21 09:13:19.74368447 +0000 UTC m=+5355.279379494",
    "heartbeat_counter": 5355,
}
HEARTBEATS_FRAME = {  # offset 19
    "channel": "heartbeats",
    "timestamp": "2026-09-21T09:13:19.743857825Z",
    "sequence_num": 19,
    "events": [HEARTBEAT],
}

SUBSCRIPTIONS_FRAME = {  # offset 0: the acknowledgement, first frame of the connection
    "channel": "subscriptions",
    "timestamp": "2026-09-21T09:13:18.816354114Z",
    "sequence_num": 0,
    "events": [{"subscriptions": {"heartbeats": ["heartbeats"]}}],
}


# --- Helpers --------------------------------------------------------------------------------


def encode(frame: dict) -> bytes:
    """Turn a frame into JSON bytes laid out exactly as the exchange sends them.

    separators=(",", ":") drops the spaces json.dumps adds by default. With it, every one of
    the 40 captured frames checked comes back byte for byte identical to what was received.

    Args:
        frame: The frame as a dict.

    Returns:
        UTF-8 JSON bytes, ready for decode_frame.
    """
    return json.dumps(frame, separators=(",", ":")).encode()


def update_frame_with(*levels: dict) -> dict:
    """Return the captured update frame, carrying the given levels instead of its own.

    Args:
        *levels: The levels the update event should carry.

    Returns:
        A new frame dict; UPDATE_FRAME itself is left alone.
    """
    return {**UPDATE_FRAME, "events": [{**UPDATE_EVENT, "updates": list(levels)}]}


def without(mapping: dict, key: str) -> dict:
    """Return a copy of a dict with one key taken out, for "missing field" cases.

    Args:
        mapping: The dict to copy.
        key: The key to leave out of the copy.

    Returns:
        A new dict; the original is left alone.
    """
    copy = dict(mapping)
    del copy[key]
    return copy


# --- Channels we model: decoded into typed frames -------------------------------------------


def test_decode_frame_snapshot_returns_l2_frame_with_snapshot_event() -> None:
    """The captured snapshot frame becomes an L2Frame carrying one Snapshot event."""
    payload = encode(SNAPSHOT_FRAME)

    frame = decode_frame(payload)

    assert type(frame) is L2Frame
    assert len(frame.events) == 1
    assert type(frame.events[0]) is Snapshot
    assert frame.events[0].product_id == "BTC-USD"


def test_decode_frame_update_returns_l2_frame_with_update_event() -> None:
    """The captured update frame becomes an L2Frame carrying one Update event."""
    payload = encode(UPDATE_FRAME)

    frame = decode_frame(payload)

    assert type(frame) is L2Frame
    assert len(frame.events) == 1
    assert type(frame.events[0]) is Update


def test_decode_frame_l2_frame_keeps_sequence_num_and_utc_timestamp() -> None:
    """The exchange's nine fraction digits become a UTC datetime, to the microsecond."""
    payload = encode(UPDATE_FRAME)

    frame = decode_frame(payload)

    assert frame.sequence_num == 2
    assert frame.timestamp == datetime(2026, 9, 21, 9, 13, 19, 54277, tzinfo=UTC)
    assert frame.timestamp.utcoffset() == timedelta(0)


def test_decode_frame_levels_keep_exact_text() -> None:
    """Prices and sizes stay strings, character for character, including a size of "0"."""
    payload = encode(UPDATE_FRAME)

    frame = decode_frame(payload)

    assert frame.events[0].updates == [
        PriceLevel(**UPDATE_BID),
        PriceLevel(**UPDATE_REMOVAL),
        PriceLevel(**UPDATE_OFFER),
    ]
    assert frame.events[0].updates[1].new_quantity == "0"


def test_decode_frame_offer_side_stays_offer() -> None:
    """The decoder keeps the exchange's word "offer"; turning it into "ask" is the book's job."""
    payload = encode(update_frame_with(UPDATE_OFFER))

    frame = decode_frame(payload)

    assert frame.events[0].updates[0].side == "offer"


def test_decode_frame_several_events_keep_their_order() -> None:
    """A frame carrying a snapshot then an update hands them back in that order."""
    payload = encode({**UPDATE_FRAME, "events": [SNAPSHOT_EVENT, UPDATE_EVENT]})

    frame = decode_frame(payload)

    assert (type(frame.events[0]), type(frame.events[1])) == (Snapshot, Update)


def test_decode_frame_event_with_no_levels_is_accepted() -> None:
    """An update that changes nothing is still a valid frame, with an empty list of levels."""
    payload = encode(update_frame_with())

    frame = decode_frame(payload)

    assert frame.events[0].updates == []


def test_decode_frame_heartbeat_returns_heartbeat_frame() -> None:
    """A heartbeat keeps Go's time text unchanged and its counter as a whole number."""
    payload = encode(HEARTBEATS_FRAME)

    frame = decode_frame(payload)

    assert type(frame) is HeartbeatFrame
    assert frame.events == [Heartbeat(**HEARTBEAT)]


def test_decode_frame_subscriptions_returns_subscriptions_frame() -> None:
    """The subscribe acknowledgement says which channels the exchange thinks we are on."""
    payload = encode(SUBSCRIPTIONS_FRAME)

    frame = decode_frame(payload)

    assert type(frame) is SubscriptionsFrame
    assert frame.events == [Subscriptions(subscriptions={"heartbeats": ["heartbeats"]})]


@pytest.mark.parametrize(
    "frame",
    [
        {**UPDATE_FRAME, "client_id": ""},
        update_frame_with({**UPDATE_BID, "order_count": 3}),
    ],
    ids=["frame-field", "level-field"],
)
def test_decode_frame_extra_fields_are_ignored(frame: dict) -> None:
    """A field the exchange adds later does not break decoding."""
    payload = encode(frame)

    decoded = decode_frame(payload)

    assert type(decoded) is L2Frame


def test_decode_frame_channel_field_last_still_decodes() -> None:
    """The frame type is picked by "channel" wherever it sits in the object."""
    payload = encode({**without(UPDATE_FRAME, "channel"), "channel": "l2_data"})  # moved to the end

    frame = decode_frame(payload)

    assert type(frame) is L2Frame


# --- Channels we do not model ---------------------------------------------------------------


@pytest.mark.parametrize(
    "channel", ["market_trades", "ticker", "level2"], ids=["market-trades", "ticker", "level2"]
)
def test_decode_frame_unmodelled_channel_returns_unknown_frame(channel: str) -> None:
    """An unmodelled channel is named rather than refused.

    "level2" is only the name used to subscribe; its data arrives on "l2_data", so a frame
    saying "level2" is unknown too.
    """
    payload = encode({**HEARTBEATS_FRAME, "channel": channel, "sequence_num": 7})

    frame = decode_frame(payload)

    assert frame == UnknownFrame(channel=channel, sequence_num=7)


def test_decode_frame_unmodelled_channel_without_counter_has_no_sequence_num() -> None:
    """An unmodelled frame with no counter reports None for it."""
    payload = encode({"channel": "ticker"})

    frame = decode_frame(payload)

    assert frame == UnknownFrame(channel="ticker", sequence_num=None)


@pytest.mark.parametrize("frame", [{}, {"type": "something"}], ids=["empty-object", "other-type"])
def test_decode_frame_object_without_channel_returns_unknown_frame(frame: dict) -> None:
    """An object with no channel that is not an error becomes an unknown frame with no name."""
    payload = encode(frame)

    decoded = decode_frame(payload)

    assert decoded == UnknownFrame(channel="", sequence_num=None)


# --- Exchange error frames ------------------------------------------------------------------


def test_decode_frame_error_returns_error_frame_with_message() -> None:
    """An exchange error carries no channel, only a type and a message."""
    payload = encode({"type": "error", "message": "failure to subscribe"})

    frame = decode_frame(payload)

    assert frame == ErrorFrame(message="failure to subscribe")


def test_decode_frame_error_without_message_returns_empty_message() -> None:
    """An error with no message still becomes an error frame, with an empty message."""
    payload = encode({"type": "error"})

    frame = decode_frame(payload)

    assert frame == ErrorFrame(message="")


def test_decode_frame_error_with_unmodelled_channel_returns_unknown_frame() -> None:
    """Only an error with no channel is an ErrorFrame; one naming another channel is unknown."""
    payload = encode({"channel": "ticker", "type": "error", "message": "failure to subscribe"})

    frame = decode_frame(payload)

    assert frame == UnknownFrame(channel="ticker", sequence_num=None)


# --- A channel we model with bad contents -> ValidationError --------------------------------


@pytest.mark.parametrize(
    "frame",
    [
        update_frame_with({**UPDATE_BID, "price_level": 83979.06}),  # a number, not text
        update_frame_with({**UPDATE_BID, "new_quantity": 0.02889672}),
        update_frame_with({**UPDATE_BID, "side": "ask"}),  # the exchange says "offer"
        update_frame_with(without(UPDATE_BID, "price_level")),
        {**UPDATE_FRAME, "events": [{**UPDATE_EVENT, "type": "trade"}]},
        without(UPDATE_FRAME, "events"),
        without(UPDATE_FRAME, "sequence_num"),
        {**UPDATE_FRAME, "sequence_num": "2"},
        {**UPDATE_FRAME, "timestamp": "yesterday"},
        {**HEARTBEATS_FRAME, "events": [{**HEARTBEAT, "heartbeat_counter": "5355"}]},
        {**SUBSCRIPTIONS_FRAME, "events": [{"subscriptions": ["heartbeats"]}]},
        {"type": "error", "channel": "l2_data", "message": "failure to subscribe"},
    ],
    ids=[
        "price-as-number",
        "size-as-number",
        "side-ask",
        "missing-price",
        "event-type-trade",
        "missing-events",
        "missing-sequence-num",
        "sequence-num-as-text",
        "timestamp-not-a-date",
        "heartbeat-counter-as-text",
        "subscriptions-not-a-mapping",
        "error-type-on-known-channel",
    ],
)
def test_decode_frame_known_channel_bad_contents_raises_validation_error(frame: dict) -> None:
    """One of our channels with broken contents fails here, before it can reach the book."""
    payload = encode(frame)

    with pytest.raises(msgspec.ValidationError):
        decode_frame(payload)


def test_decode_frame_known_channel_bad_contents_names_the_field() -> None:
    """The error says which field was wrong, so the [invalid] log line points at it."""
    payload = encode(update_frame_with({**UPDATE_BID, "price_level": 83979.06}))
    field_path = r"\$\.events\[0\]\.updates\[0\]\.price_level"  # $.events[0].updates[0].price_level

    with pytest.raises(msgspec.ValidationError, match=field_path):
        decode_frame(payload)


# --- Valid JSON that is not shaped like a frame -> ValidationError --------------------------


@pytest.mark.parametrize(
    "payload",
    [
        b"[]",
        b"42",
        b'"text"',
        b"null",
        b'{"channel":5}',
        b'{"type":5}',
        b'{"type":"error","message":5}',
        b'{"channel":"ticker","sequence_num":"7"}',
    ],
    ids=[
        "array",
        "number",
        "string",
        "null",
        "channel-not-text",
        "type-not-text",
        "error-message-not-text",
        "unmodelled-counter-as-text",
    ],
)
def test_decode_frame_not_shaped_like_a_frame_raises_validation_error(payload: bytes) -> None:
    """Valid JSON that is not shaped like a frame is refused as well, as the docstring says.

    None of these is one of our channels. The second, smaller read decode_frame takes (the
    private _Probe) needs a JSON object with text channel, type and message and a whole-number
    sequence_num, and its own error escapes when those are not met. handle() in
    src/book/main.py catches it and logs [invalid], so nothing reaches the book.
    """
    with pytest.raises(msgspec.ValidationError):
        decode_frame(payload)


# --- Not JSON -> DecodeError ----------------------------------------------------------------


@pytest.mark.parametrize(
    "payload",
    [b"", b"not json", b'{"channel":"l2_data"', b"\xff\xfe"],
    ids=["empty", "plain-text", "cut-off", "invalid-bytes"],
)
def test_decode_frame_not_json_raises_decode_error(payload: bytes) -> None:
    """Bytes that are not JSON raise DecodeError itself.

    ValidationError is a subclass of DecodeError, so pytest.raises(DecodeError) alone would
    also pass on a ValidationError. The exact type is checked to tell the two apart.
    """
    with pytest.raises(msgspec.DecodeError) as caught:
        decode_frame(payload)

    assert caught.type is msgspec.DecodeError
