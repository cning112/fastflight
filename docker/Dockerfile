# Multi-stage build for optimized production image
FROM python:3.11-slim as builder

# Install uv for fast dependency management  
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Set working directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies in virtual environment
RUN uv sync --frozen --no-dev

# Production stage
FROM python:3.11-slim

# Install uv in production image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# Copy source code
COPY src ./src

# Set environment variables
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app/src"

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import fastflight; print('OK')" || exit 1

# Use CLI as entry point
ENTRYPOINT ["fastflight"]
CMD ["start-fast-flight-server"]
