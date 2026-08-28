# Image for the ingest service: captures the Coinbase Level 2 feed for BTC-USD.
# Build context is the repository root, not this folder -- see docker-compose.yml.
FROM python:3.14.7-slim-trixie

# Send print() output straight through so `docker logs` shows capture progress live.
ENV PYTHONUNBUFFERED=1
# Skip writing .pyc files; the container filesystem is thrown away after every run.
ENV PYTHONDONTWRITEBYTECODE=1

# Everything below lives under /app, so RAW_DIR = Path("raw") resolves to /app/raw.
WORKDIR /app

# Copy the requirements on their own first, so the install layer survives source edits.
COPY requirements.txt .
# Install the pinned dependencies; --no-cache-dir keeps the wheel cache out of the image.
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source last, because it changes most often and invalidates the fewest layers.
COPY src/ src/

# Default run: one capture using the built-in default duration.
CMD ["python", "-m", "src.ingest.main"]
