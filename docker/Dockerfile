FROM python:3.12-slim AS base
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app


FROM base AS development
ENV PATH="/app/.venv/bin:$PATH"

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --all-extras --no-install-project

ADD src /app

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=README.md,target=README.md \
    uv sync --frozen --all-extras

ENTRYPOINT ["/bin/bash"]


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
#    echo "=== !!! More aggressive cleanup (disabled) !!! ===" && \
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


FROM python:3.12-slim as production

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

RUN echo "=== Create non-root user ===" && \
    groupadd -r -g 1001 fastflight && \
    useradd -r -u 1001 -g fastflight fastflight

# Copy files as root and set correct ownership
COPY --from=builder --chown=fastflight:fastflight /app/.venv /app/.venv

USER fastflight

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import numpy, pandas, pyarrow, fastflight; print('OK')" || exit 1

ENTRYPOINT ["fastflight"]
CMD ["start-all"]
