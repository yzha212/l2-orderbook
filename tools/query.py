"""Open DuckDB on the archived order book Parquet files, with the helper views ready.

How to use this:

1. One-time setup. DuckDB is a host-side analysis tool and is deliberately not in
   requirements.txt, so install it into the local venv once:

       .venv/bin/pip install duckdb==1.5.5

2. Interactive session. `-i` runs this file and then drops you at a Python prompt with
   `con` already connected and the views already built:

       .venv/bin/python -i tools/query.py

       >>> con.sql("SELECT * FROM book_l1 ORDER BY published_at DESC LIMIT 10")
       >>> con.sql("DESCRIBE book")
       >>> con.sql("SELECT count(*) FROM book")

   Results print as a boxed table. Ctrl-D leaves the session.

3. A single query from the shell, no prompt:

       .venv/bin/python tools/query.py "SELECT count(*) FROM book"

4. Where the data comes from. The views read
   data/book/symbol=BTC-USD/date=YYYY-MM-DD/*.parquet directly -- nothing is imported and
   no database file is written, so the Parquet files stay the only copy. Any directory
   works as the working directory; see SET file_search_path below.

5. Running this while the archiver is running is safe. The archiver writes
   <name>.parquet.part and renames it into place, so the *.parquet glob never picks up a
   half-written file. New buckets simply appear in the next query.

See tools/duckdb_init.sql for the view definitions and more example queries.
"""

import sys
from pathlib import Path

import duckdb

# The init script globs relative to the repository root, which is this file's grandparent.
ROOT = Path(__file__).resolve().parent.parent


def open_book() -> duckdb.DuckDBPyConnection:
    """Connect to an in-memory DuckDB holding views over the Parquet archive.

    Returns:
        A connection with the `book` and `book_l1` views already defined.
    """
    con = duckdb.connect()
    # Resolve the init script's relative globs against the repo root, whatever the CWD is.
    con.execute(f"SET file_search_path = '{ROOT}'")
    # The files store UTC; render UTC too rather than the machine's local zone.
    con.execute("SET TimeZone = 'UTC'")
    con.execute((ROOT / "tools" / "duckdb_init.sql").read_text())
    return con


con = open_book()

# A query on the command line runs once; with no argument `python -i` leaves `con` ready.
if len(sys.argv) > 1:
    con.sql(sys.argv[1]).show()
else:
    print("Views: book, book_l1.  Try: con.sql('SELECT * FROM book_l1 LIMIT 5')")
