-- Views over the archived top-of-book Parquet files, loaded by tools/query.py.
-- Run them with:  .venv/bin/python -i tools/query.py
-- Paths below are relative to the repository root; query.py points DuckDB at it for you.

-- Every archived row. hive_partitioning recovers symbol and date from the directory names,
-- which adds them as columns and lets a WHERE on date skip whole files.
CREATE OR REPLACE VIEW book AS
SELECT *
FROM read_parquet(
    'data/book/symbol=*/date=*/*.parquet',
    hive_partitioning = true
);

-- Best bid and ask only: the usual starting point for a look at the data.
-- Level index 00 is the best level; a missing level is NULL, never zero.
CREATE OR REPLACE VIEW book_l1 AS
SELECT
    published_at,
    timestamp,
    doubtful,
    bid_px_00 AS best_bid,
    ask_px_00 AS best_ask,
    ask_px_00 - bid_px_00 AS spread,
    (bid_px_00 + ask_px_00) / 2 AS mid,
    bid_sz_00 AS best_bid_size,
    ask_sz_00 AS best_ask_size,
    symbol,
    date
FROM book;

-- Example queries. Paste one into the session; none of them are run by this script.
--
-- What is in the archive, per day:
--     SELECT date, count(*) AS rows, min(published_at), max(published_at),
--            count(*) FILTER (WHERE doubtful) AS doubtful
--     FROM book GROUP BY date ORDER BY date;
--
-- Filtering on date prunes whole files; EXPLAIN shows it as "File Filters":
--     SELECT * FROM book_l1 WHERE date = DATE '2026-09-10' LIMIT 20;
--
-- Crossed books. Among trustworthy rows this must return nothing -- every crossed row in
-- the archive so far is one the builder had already flagged doubtful:
--     SELECT * FROM book_l1 WHERE best_bid >= best_ask AND NOT doubtful;
--
-- The full depth of one row, widest first:
--     SELECT * FROM book ORDER BY published_at DESC LIMIT 1;
--
-- Per-file health, straight from the writer's own l2.* file metadata:
--     SELECT regexp_extract(file_name, '[^/]+$') AS file, decode(key) AS k, decode(value) AS v
--     FROM parquet_kv_metadata('data/book/symbol=*/date=*/*.parquet')
--     WHERE decode(key) IN ('l2.rows', 'l2.doubtful_rows', 'l2.offset_gaps')
--     ORDER BY file DESC;
