# Base stage: Shared foundation for all targets
# Contains Python runtime, uv package manager, and common environment settings
FROM python:3.12-slim AS base
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app


# Development target: Full development environment for interactive debugging and remote development
# Use cases:
# - Remote server development where IDE is not available (SSH + vi/nano)
# - CI/CD pipeline debugging and troubleshooting
# - Emergency hotfixes in production-like environments
# - Containerized development for team consistency
FROM base AS development
ENV PATH="/app/.venv/bin:$PATH"

# Install ALL dependencies including dev tools (pytest, black, mypy, etc.)
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --all-extras --no-install-project

ADD src /app

# Install project in EDITABLE mode for live code changes
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=README.md,target=README.md \
    uv sync --frozen --all-extras

# Keep as root user for development flexibility (installing additional tools, etc.)
# NO cleanup - preserve docs, examples, tests for reference and debugging
# Provides interactive bash shell for manual operations
ENTRYPOINT ["/bin/bash"]


# Builder stage: Optimized production build with aggressive cleanup
# Creates the smallest possible production-ready virtual environment
FROM base AS builder
# Install dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --all-extras --no-install-project --no-dev --compile-bytecode --no-editable

# Copy the project into the intermediate image
ADD src /app

# Sync the project
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=README.md,target=README.md \
    uv sync --frozen --all-extras --no-dev --compile-bytecode --no-editable

# Aggressive cleanup to minimize image size while preserving functionality
# Safe to remove: documentation, examples, license files, cache files
RUN echo "=== Cleanup docs and examples (safe) ===" && \
    find .venv/lib/python*/site-packages -name "docs"       -type d -exec rm -rf {} + 2>/dev/null || true && \
    find .venv/lib/python*/site-packages -name "examples"   -type d -exec rm -rf {} + 2>/dev/null || true && \
    find .venv/lib/python*/site-packages -name "tests"      -type d -exec rm -rf {} + 2>/dev/null || true && \
    find .venv/lib/python*/site-packages -name "*.md"       -delete 2>/dev/null || true && \
    find .venv/lib/python*/site-packages -name "*.rst"      -delete 2>/dev/null || true && \
    find .venv/lib/python*/site-packages -name "*.txt"      -delete 2>/dev/null || true && \
    find .venv/lib/python*/site-packages -name "LICENSE*"   -delete 2>/dev/null || true && \
    find .venv/lib/python*/site-packages -name "COPYING*"   -delete 2>/dev/null || true && \
    echo "=== Size optimization cleanup ===" && \
    find .venv/lib/python*/site-packages -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true && \
    find .venv/lib/python*/site-packages -name "*.pyc"      -delete 2>/dev/null || true && \
    find .venv/lib/python*/site-packages -name "*.pyo"      -delete 2>/dev/null || true && \
#    echo "=== !!! More aggressive cleanup (disabled for safety) !!! ===" && \
#    echo "=== The following cleanup steps are commented out to avoid breaking package functionality ===" && \
#    echo "=== Cleanup development files (don't delete test modules) ===" && \
#    find .venv/lib/python*/site-packages -name "*.dist-info" -type d -exec rm -rf {} + 2>/dev/null || true && \
#    find .venv/lib/python*/site-packages -name "*.egg-info" -type d -exec rm -rf {} + 2>/dev/null || true && \
#    echo "=== Cleanup header files and static libraries ===" && \
#    find .venv/lib/python*/site-packages -name "include"    -type d -exec rm -rf {} + 2>/dev/null || true && \
#    find .venv/lib/python*/site-packages -name "*.a"        -delete 2>/dev/null || true && \
#    find .venv/lib/python*/site-packages -name "*.h"        -delete 2>/dev/null || true && \
#    find .venv/lib/python*/site-packages -name "*.hpp"      -delete 2>/dev/null || true && \
#    echo "=== Strip shared libraries ===" && \
#    find .venv/lib/python*/site-packages -name "*.so*"      -exec strip --strip-unneeded {} \; 2>/dev/null || true \
    echo "=== Final verification ===" && \
    uv run python -c "import numpy, pandas, pyarrow, fastflight; print('✅ Safe cleanup complete, all packages working normally')"


# Production target: Minimal runtime image optimized for deployment
# Final size: ~543MB with numpy, pandas, pyarrow, and fastflight
# Security: runs as non-root user, minimal attack surface
FROM python:3.12-slim as production

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# Create dedicated non-root user for security
RUN echo "=== Create non-root user ===" && \
    groupadd -r -g 1001 fastflight && \
    useradd -r -u 1001 -g fastflight fastflight

# Copy only the cleaned virtual environment from builder stage
COPY --from=builder --chown=fastflight:fastflight /app/.venv /app/.venv

USER fastflight

# Health check ensures service availability for orchestration systems
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import numpy, pandas, pyarrow, fastflight; print('OK')" || exit 1

# Default: start all services (FastFlight + REST API)
ENTRYPOINT ["fastflight"]
CMD ["start-all"]
