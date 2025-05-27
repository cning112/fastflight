FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Copy project files
COPY requirements.txt pyproject.toml README.md src ./

# Install dependencies, build app, and cleanup in one layer
RUN echo "=== Installing system dependencies ===" && \
    apt-get update && \
    apt-get install -y --no-install-recommends gcc g++ && \
    echo "=== Installing Python dependencies ===" && \
    pip install --no-cache-dir -r requirements.txt && \
    echo "=== Installing FastFlight package ===" && \
    pip install --no-cache-dir --no-deps . && \
    echo "=== Verifying installation ===" && \
    python -c "import numpy, pandas, pyarrow, fastflight; print('✅ Installation successful')" && \
    echo "=== Cleaning up build tools ===" && \
    apt-get remove -y gcc g++ && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    echo "=== Cleanup docs and examples (safe) ===" && \
    find /usr/local/lib/python*/site-packages -name "docs" -type d -exec rm -rf {} + 2>/dev/null || true && \
    find /usr/local/lib/python*/site-packages -name "examples" -type d -exec rm -rf {} + 2>/dev/null || true && \
    find /usr/local/lib/python*/site-packages -name "*.md" -delete 2>/dev/null || true && \
    find /usr/local/lib/python*/site-packages -name "*.rst" -delete 2>/dev/null || true && \
    find /usr/local/lib/python*/site-packages -name "*.txt" -delete 2>/dev/null || true && \
    find /usr/local/lib/python*/site-packages -name "LICENSE*" -delete 2>/dev/null || true && \
    find /usr/local/lib/python*/site-packages -name "COPYING*" -delete 2>/dev/null || true && \
    echo "=== Cleanup compilation cache ===" && \
    find /usr/local/lib/python*/site-packages -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true && \
    find /usr/local/lib/python*/site-packages -name "*.pyc" -delete 2>/dev/null || true && \
    find /usr/local/lib/python*/site-packages -name "*.pyo" -delete 2>/dev/null || true && \
    echo "=== Cleanup development files (don't delete test modules) ===" && \
    find /usr/local/lib/python*/site-packages -name "*.dist-info" -type d -exec rm -rf {} + 2>/dev/null || true && \
    find /usr/local/lib/python*/site-packages -name "*.egg-info" -type d -exec rm -rf {} + 2>/dev/null || true && \
    echo "=== Cleanup header files and static libraries ===" && \
    find /usr/local/lib/python*/site-packages -path "**/include" -type d -exec rm -rf {} + 2>/dev/null || true && \
    find /usr/local/lib/python*/site-packages -name "*.a" -delete 2>/dev/null || true && \
    find /usr/local/lib/python*/site-packages -name "*.h" -delete 2>/dev/null || true && \
    find /usr/local/lib/python*/site-packages -name "*.hpp" -delete 2>/dev/null || true && \
    echo "=== Strip shared libraries ===" && \
    find /usr/local/lib/python*/site-packages -name "*.so*" -exec strip --strip-unneeded {} \; 2>/dev/null || true && \
    echo "=== Final verification ===" && \
    python -c "import numpy, pandas, pyarrow, fastflight; print('✅ Safe cleanup complete, all packages working normally')"

RUN echo "=== Create non-root user ===" && \
    groupadd -r -g 1001 fastflight && \
    useradd -r -u 1001 -g fastflight fastflight && \
    chown -R fastflight:fastflight /app

USER fastflight

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import numpy; import pandas; import fastflight; print('OK')" || exit 1

# Entry point
ENTRYPOINT ["fastflight"]
CMD ["start-fast-flight-server"]