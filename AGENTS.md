# Coding Agent Instructions

This repository contains the components that ingests BTC-USD (one symbol only) Level 2 orderbook data stream from Coinbase Advanced Trade Websocket endpoint, then builds and stores the order book.

## Project Setup
- Dependency management: `pip` and `requirements.txt`, do NOT use other package manager  
- Python 3.14, pinned via the `python:3.14.7-slim-trixie` base image; code targets 3.14+ syntax
- Branch from `main` as `feature/<name>` and use Coventional Commits


## Coding Conventions
- Always use English for all comments.
- Type-hint public functions and methods, including their return types.
- Use `pathlib` for path management. Don't use `os.path`.
- Prefer f-strings over `str.format()` or `%` formatting.
- Follow EAFP: handle exceptions rather than checking conditions up front.
- Write Google-style docstrings for every public function and method.
- Embrace simple idiomatic Python like comprehensions, generators, and decorators.

## Architecture
- High-level component reference: [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md)
