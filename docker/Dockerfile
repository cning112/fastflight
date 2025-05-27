# Multi-stage build for optimized production image
FROM python:3.11-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Set working directory
WORKDIR /app

# Copy dependency files
COPY requirements.txt ./

# Install dependencies
RUN uv pip install -r requirements.txt --system --no-cache --cache-dir /tmp/uv-cache
RUN rm -fr /tmp/uv-cache

# Copy source code
COPY src ./src
ENV PYTHONPATH="/app/src"

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import fastflight; print('OK')" || exit 1

# Use CLI as entry point
ENTRYPOINT ["python", "-m", "fastflight.cli"]
CMD ["start-fast-flight-server"]
