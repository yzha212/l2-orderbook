"""Capture the Coinbase Advanced Trade Level 2 feed for BTC-USD into gzipped JSONL.

This is an exploration spike: everything lives in this one file, and every line carries
a short comment so the flow can be read top to bottom.
"""

import argparse  # parses the --duration flag from the command line
import gzip  # writes the capture file compressed, without a third-party library
import json  # turns our subscribe dicts into JSON text to send over the socket
import shutil  # gives us rmtree, used to wipe the raw folder before each run
import threading  # provides Timer, which stops the capture after --duration seconds
import time  # measures how long the run actually lasted
from datetime import datetime, timezone  # builds the UTC timestamp in the output filename
from pathlib import Path  # all filesystem paths are Path objects, never strings

import websocket  # the "websocket-client" package used by the Coinbase docs example

WS_URL = "wss://advanced-trade-ws.coinbase.com"  # public market data endpoint, no auth needed
PRODUCT_ID = "BTC-USD"  # the single symbol this project ingests
RAW_DIR = Path("raw")  # folder the capture files are written into
BATCH_RECORDS = 500  # how many messages to hold in memory before one write to disk
DEFAULT_DURATION = 20.0  # seconds to capture when --duration is not given
COMPRESS_LEVEL = 1  # weakest gzip setting: fastest, still shrinks the feed a lot
PING_INTERVAL = 20  # send a keepalive ping every 20 seconds
PING_TIMEOUT = 10  # give up on a ping that goes unanswered for 10 seconds


def prepare_raw_dir(raw_dir: Path) -> Path:
    """Delete the raw folder and recreate it empty.

    Args:
        raw_dir: Folder that will hold this run's capture file.

    Returns:
        The resolved absolute path to the freshly created folder.

    Raises:
        ValueError: If the path resolves to a filesystem root, which must never be wiped.
    """
    resolved = raw_dir.resolve()  # turn a relative path like "raw" into a full absolute path
    if resolved.parent == resolved:  # a path whose parent is itself is a filesystem root
        raise ValueError(f"refusing to wipe filesystem root: {resolved}")  # stop before damage
    shutil.rmtree(resolved, ignore_errors=True)  # remove the folder and everything inside it
    resolved.mkdir(parents=True, exist_ok=True)  # recreate it empty, ready for this run
    return resolved  # hand the absolute path back to the caller


def build_capture_path(raw_dir: Path) -> Path:
    """Build the filename for this run's capture.

    Args:
        raw_dir: Folder the file will live in.

    Returns:
        A path like ``raw/BTC-USD_20260827T043432Z.jsonl.gz``.
    """
    now = datetime.now(timezone.utc)  # take the current time, explicitly in UTC
    stamp = now.strftime("%Y%m%dT%H%M%SZ")  # format it compactly so it sorts alphabetically
    return raw_dir / f"{PRODUCT_ID}_{stamp}.jsonl.gz"  # join folder and filename with pathlib


def capture(duration: float, raw_dir: Path = RAW_DIR) -> None:
    """Connect, subscribe, and record the feed until the time is up.

    Messages are buffered in a list and written in batches, so the process touches
    the disk once per ``BATCH_RECORDS`` messages instead of once per message.

    Args:
        duration: How many seconds to stay connected before closing.
        raw_dir: Folder to wipe and write the capture into.
    """
    folder = prepare_raw_dir(raw_dir)  # wipe and recreate the output folder first
    path = build_capture_path(folder)  # decide what this run's file is called
    buffer: list[str] = []  # holds message lines that have not reached the disk yet
    stats = {"messages": 0, "writes": 0}  # a dict so the inner callbacks can mutate the counts
    handle = gzip.open(path, "wt", encoding="utf-8", compresslevel=COMPRESS_LEVEL)  # open gzip

    def flush() -> None:
        """Write everything currently buffered to the file in a single call."""
        if not buffer:  # nothing pending means there is no reason to touch the disk
            return  # leave the counters alone and go back
        handle.write("".join(buffer))  # one write call carrying the whole batch of lines
        buffer.clear()  # empty the buffer so the next batch starts fresh
        stats["writes"] += 1  # remember that one more batch write happened

    def on_open(ws: websocket.WebSocketApp) -> None:
        """Send our subscriptions as soon as the socket is connected."""
        # heartbeats = {"type": "subscribe", "channel": "heartbeats"}  # keeps a quiet link alive
        # ws.send(json.dumps(heartbeats))  # serialize the dict to JSON text and send it
        level2 = {"type": "subscribe", "product_ids": [PRODUCT_ID], "channel": "level2"}  # book
        ws.send(json.dumps(level2))  # note we SEND "level2"; the replies come back as "l2_data"
        print(f"subscribed to level2 for {PRODUCT_ID}")  # tell the user the stream has started

    def on_message(ws: websocket.WebSocketApp, message: str) -> None:
        """Buffer one incoming frame, flushing to disk when the batch is full."""
        buffer.append(f"{message}\n")  # store the frame verbatim as one line, nothing re-encoded
        stats["messages"] += 1  # count every message we received
        if len(buffer) >= BATCH_RECORDS:  # only go to the disk once enough lines have piled up
            flush()  # write the whole batch in one go

    def on_error(ws: websocket.WebSocketApp, error: Exception) -> None:
        """Report a socket-level error, treating Ctrl+C as a normal stop."""
        if isinstance(error, KeyboardInterrupt):  # run_forever routes Ctrl+C here, not to except
            print("\n[interrupted] closing early")  # say so plainly instead of calling it an error
            return  # a deliberate stop is not a failure, so print nothing more
        print(f"[error] {error}")  # surface a real problem so it is visible in the terminal

    def on_close(ws: websocket.WebSocketApp, status_code: int | None, msg: str | None) -> None:
        """Report that the connection has ended."""
        print(f"[closed] status={status_code} message={msg}")  # show why the socket went away

    app = websocket.WebSocketApp(  # build the client with our four callbacks attached
        WS_URL,  # the public endpoint to dial
        on_open=on_open,  # runs once, right after connecting
        on_message=on_message,  # runs for every frame the server sends
        on_error=on_error,  # runs if the connection fails
        on_close=on_close,  # runs when the connection ends
    )
    timer = threading.Timer(duration, app.close)  # schedule app.close() for `duration` from now
    started = time.monotonic()  # record the start on a clock that never jumps backwards
    timer.start()  # arm the timer; it runs on its own background thread
    try:  # EAFP: just run, and deal with an interruption if one actually happens
        app.run_forever(ping_interval=PING_INTERVAL, ping_timeout=PING_TIMEOUT)  # blocking loop
    except KeyboardInterrupt:  # the user pressed Ctrl+C before the timer fired
        print("\n[interrupted] closing early")  # acknowledge the interrupt on its own line
        app.close()  # ask the socket to shut down cleanly
    finally:  # this block runs whether we finished normally, errored, or were interrupted
        timer.cancel()  # stop the timer so a stray close() cannot fire later
        flush()  # write whatever partial batch is still sitting in memory
        handle.close()  # close the gzip stream so the file is complete and readable
        elapsed = time.monotonic() - started  # how long the capture actually lasted
        size = path.stat().st_size  # the compressed size of the finished file, in bytes
        print(f"captured {stats['messages']} messages in {elapsed:.1f}s")  # message throughput
        print(f"wrote {stats['writes']} batches to {path} ({size / 1024:.1f} KiB)")  # file result


def main() -> None:
    """Parse the command line and run one capture."""
    parser = argparse.ArgumentParser(description=__doc__)  # reuse the module docstring as help
    parser.add_argument(  # the single flag this spike exposes
        "--duration",  # name of the flag on the command line
        type=float,  # accept fractional seconds like 2.5
        default=DEFAULT_DURATION,  # short default keeps test runs quick
        help="seconds to capture before closing the connection",  # shown by --help
    )
    args = parser.parse_args()  # read the actual arguments the user typed
    capture(args.duration)  # run the capture with that duration


if __name__ == "__main__":  # only run when executed directly, not when imported
    main()  # start the program
