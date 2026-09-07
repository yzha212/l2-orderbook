"""Turn raw Coinbase Advanced Trade frames into typed objects.

This is stage 3 of the architecture: the one boundary where raw JSON becomes typed objects,
so nothing downstream ever touches raw text. It is a library rather than a service -- there
is no Dockerfile and no main here; the reader and the book builder import it.
"""

from datetime import datetime
from typing import Literal

import msgspec

BOOK_CHANNEL = "l2_data"  # the only channel carrying snapshot and update events
KNOWN_CHANNELS = frozenset({BOOK_CHANNEL, "heartbeats", "subscriptions"})  # channels modelled here


class PriceLevel(msgspec.Struct, frozen=True, gc=False):
    """One price level from a book snapshot or update.

    Frozen because the book builder reads these and never edits them, and untracked by the
    garbage collector because a snapshot carries tens of thousands of them and a struct of
    plain strings can never take part in a reference cycle.

    Attributes:
        side: Which half of the book; the exchange says "offer" where the book says ask.
        event_time: When the exchange applied the change, exactly as it sent it.
        price_level: The price being changed, kept as a string end to end.
        new_quantity: The size now resting there, not a delta; "0" removes the level.
    """

    side: Literal["bid", "offer"]
    event_time: str
    price_level: str
    new_quantity: str


class Snapshot(msgspec.Struct, tag="snapshot"):
    """The opening state of the book: every level the exchange currently holds.

    Identical in shape to Update on purpose. They are separate types so the book builder can
    tell them apart: a snapshot replaces the book, an update edits it.

    Attributes:
        product_id: The symbol these levels belong to.
        updates: Every resting level, to be loaded as the starting book.
    """

    product_id: str
    updates: list[PriceLevel]


class Update(msgspec.Struct, tag="update"):
    """A batch of changed levels to apply on top of the book already loaded.

    Attributes:
        product_id: The symbol these levels belong to.
        updates: The levels that changed; a new_quantity of "0" removes one.
    """

    product_id: str
    updates: list[PriceLevel]


class L2Frame(msgspec.Struct, tag="l2_data", tag_field="channel"):
    """A level2 frame: the envelope the book builder reads its events from.

    Attributes:
        sequence_num: The exchange's own counter, used later to spot missed frames.
        timestamp: When the exchange sent the frame.
        events: The snapshot or update events this frame carries.
    """

    sequence_num: int
    timestamp: datetime
    events: list[Snapshot | Update]


class Heartbeat(msgspec.Struct):
    """One heartbeat tick.

    Attributes:
        current_time: The exchange's own clock, exactly as it sent it. Kept as a string
            because it arrives in Go's own time format, not ISO 8601.
        heartbeat_counter: Counts up once a second, so a gap means frames were missed.
            The exchange sends it as a JSON number, and it keeps counting across
            connections rather than restarting at zero.
    """

    current_time: str
    heartbeat_counter: int


class HeartbeatFrame(msgspec.Struct, tag="heartbeats", tag_field="channel"):
    """A heartbeat frame, which arrives only while the heartbeats channel is subscribed.

    Attributes:
        sequence_num: The exchange's own counter.
        timestamp: When the exchange sent the frame.
        events: The heartbeat ticks this frame carries.
    """

    sequence_num: int
    timestamp: datetime
    events: list[Heartbeat]


class Subscriptions(msgspec.Struct):
    """The set of channels the exchange believes we are subscribed to.

    Attributes:
        subscriptions: Channel name to the product ids subscribed on it.
    """

    subscriptions: dict[str, list[str]]


class SubscriptionsFrame(msgspec.Struct, tag="subscriptions", tag_field="channel"):
    """The acknowledgement the exchange sends after every subscribe and unsubscribe.

    Attributes:
        sequence_num: The exchange's own counter.
        timestamp: When the exchange sent the frame.
        events: What the exchange now considers subscribed.
    """

    sequence_num: int
    timestamp: datetime
    events: list[Subscriptions]


class ErrorFrame(msgspec.Struct):
    """An error the exchange sent in place of a normal frame.

    These carry no channel field at all, so they cannot join the tagged union that the other
    frames form; decode_frame builds them by hand instead.

    Attributes:
        message: What the exchange said went wrong.
        channel: Fixed, so that every frame type answers channel_of the same way.
    """

    message: str
    channel: str = "error"


class UnknownFrame(msgspec.Struct):
    """A frame from a channel this module does not model yet.

    Not malformed, just unexpected: a new exchange channel, or one nobody has needed so far.
    Kept as a type of its own so an unmodelled channel is reported rather than passed on.

    Attributes:
        channel: The channel the exchange named.
        sequence_num: The frame's counter, when it carried one.
    """

    channel: str
    sequence_num: int | None = None


class _Probe(msgspec.Struct):
    """The few fields needed to classify a frame the tagged union rejected.

    Every field is optional and msgspec ignores the rest, so this decodes any JSON object at
    all and only ever reports what the frame claimed to be.
    """

    channel: str | None = None
    type: str | None = None
    message: str | None = None
    sequence_num: int | None = None


KnownFrame = L2Frame | HeartbeatFrame | SubscriptionsFrame  # tagged union keyed on "channel"
Frame = KnownFrame | ErrorFrame | UnknownFrame  # everything decode_frame can hand back

_frames = msgspec.json.Decoder(KnownFrame)  # built once, so the schema compiles once
_probe = msgspec.json.Decoder(_Probe)


def decode_frame(payload: bytes) -> Frame:
    """Decode one raw frame into a typed object.

    Args:
        payload: The frame exactly as the exchange sent it, as UTF-8 bytes.

    Returns:
        A typed frame. Channels this module models each become their own type; a channel it
        does not model becomes an UnknownFrame carrying the channel name.

    Raises:
        msgspec.ValidationError: The frame came from a channel we model, but its contents do
            not match the schema.
        msgspec.DecodeError: The payload is not valid JSON.
    """
    try:  # EAFP: nearly every frame is a channel we model, so decode first and ask later
        return _frames.decode(payload)
    except msgspec.ValidationError:  # either an unmodelled channel, or contents we refuse
        probe = _probe.decode(payload)
        if probe.channel in KNOWN_CHANNELS:  # a channel we do model, so the contents are wrong
            raise  # stage 3 fails here rather than let a bad frame corrupt the book
        if probe.channel is None and probe.type == "error":  # errors carry no channel at all
            return ErrorFrame(message=probe.message or "")
        return UnknownFrame(channel=probe.channel or "", sequence_num=probe.sequence_num)


def channel_of(frame: Frame) -> str:
    """Return the Coinbase channel name a decoded frame came from.

    The tagged frames keep their channel on the class rather than the instance, because
    msgspec forbids a struct field that shares the tag field's name.

    Args:
        frame: The typed frame decode_frame returned.

    Returns:
        The channel name, from the class tag for tagged frames and from the field for the
        two untagged ones.
    """
    return type(frame).__struct_config__.tag or frame.channel


def event_type(event: Snapshot | Update) -> str:
    """Return which kind of book event this is.

    Args:
        event: One event taken from an L2Frame.

    Returns:
        Either "snapshot" or "update", read back off the class tag.
    """
    return type(event).__struct_config__.tag
