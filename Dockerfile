FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install system deps needed by python-docx (lxml) build wheels.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Layer caching: install dependencies before copying source so a code change
# doesn't bust the dependency layer.
COPY pyproject.toml ./
COPY requirements.txt ./
RUN pip install -r requirements.txt

# Copy application source.
COPY src ./src
COPY .env.example ./

# Run as non-root for safety.
RUN useradd --create-home --shell /bin/bash agent \
    && chown -R agent:agent /app
USER agent

# State files (token.json, processed_transcripts.json, dead_letter_queue.json)
# are mounted from the host via docker-compose, so they survive container restarts.
VOLUME ["/app/state"]

ENV PROCESSED_TRANSCRIPTS_PATH=/app/state/processed_transcripts.json \
    DEAD_LETTER_PATH=/app/state/dead_letter_queue.json \
    GOOGLE_TOKEN_PATH=/app/state/token.json \
    GOOGLE_CREDENTIALS_PATH=/app/state/credentials.json

CMD ["python", "-m", "src.scheduler.main"]
