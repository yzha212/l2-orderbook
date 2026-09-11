# Image for the dashboard service: shows the published top of the book in the terminal.
# Build context is the repository root, not this folder -- see docker-compose.yml.
FROM python:3.14.7-slim-trixie

# Send print() output straight through, so the live display is not held in a buffer.
ENV PYTHONUNBUFFERED=1
# Skip writing .pyc files; the image is rebuilt rather than updated in place.
ENV PYTHONDONTWRITEBYTECODE=1

# Everything below lives under /app, so `python -m src.dashboard.main` resolves from there.
WORKDIR /app

# Copy the requirements on their own first, so the install layer survives source edits.
COPY requirements.txt .
# Install the pinned dependencies; --no-cache-dir keeps the wheel cache out of the image.
RUN pip install --no-cache-dir --timeout 120 --retries 10 -r requirements.txt

# Copy the source last, because it changes most often and invalidates the fewest layers.
COPY src/ src/

# Default run: draw the ladder until the container is stopped.
CMD ["python", "-m", "src.dashboard.main"]
