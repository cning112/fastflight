# ---- Builder Stage ----
FROM python:3.10-slim-buster AS builder

# Set working directory for virtual environment
WORKDIR /opt/venv

# Create virtual environment
RUN python -m venv .
ENV PATH="/opt/venv/bin:$PATH" # Activate venv

# Install dependencies
COPY requirements.txt .
# Ensure pip is up-to-date and install wheel for potentially compiled packages
RUN pip install --no-cache-dir --upgrade pip wheel
RUN pip install --no-cache-dir -r requirements.txt


# ---- Final Stage ----
FROM python:3.10-slim-buster

# Set environment variables for Python
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Install curl for health checks and any other essential OS packages
# Also update apt-get and clean up to keep image size down
RUN apt-get update && \
    apt-get install -y curl --no-install-recommends && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create a non-root user and group
ARG APP_USER=fastflight_user
ARG APP_GROUP=fastflight_user
RUN groupadd -r ${APP_GROUP} && useradd --no-log-init -r -g ${APP_GROUP} ${APP_USER}

# Copy the virtual environment from the builder stage
COPY --from=builder /opt/venv /opt/venv

# Set up working directory and copy application code
WORKDIR /app
# Copy the src directory and any other necessary files
COPY --chown=${APP_USER}:${APP_GROUP} src /app/src

# Set up environment for the application
ENV PATH="/opt/venv/bin:$PATH" # Activate venv for the final stage
ENV PYTHONPATH="/app" # Application source code is in /app/src, so /app is the root for imports

# Switch to the non-root user
USER ${APP_USER}

# Expose default ports
EXPOSE 8815 # Default for Flight Server (FASTFLIGHT_SERVER_PORT)
EXPOSE 8000 # Default for FastAPI (FASTFLIGHT_API_PORT)

# Health check for FastAPI (assumes FastAPI runs on port 8000)
# The CMD should ideally run `start-all` or ensure FastAPI is running.
# If only Flight server runs, this health check might fail or be irrelevant.
# This assumes the default CMD will be changed to run both, or this Dockerfile is for FastAPI.
# For now, adding it with the assumption that the CMD will eventually run the FastAPI server.
# TODO: Make health check ports configurable via ARG or tied to ENV if possible, though HEALTHCHECK doesn't directly expand ENV.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:8000/fastflight/health || exit 1

# Health check for Flight Server (simple TCP check on port 8815)
# This assumes the Flight server runs on port 8815.
# TODO: Similar to FastAPI, port should be configurable.
# Using a Python snippet for TCP check as netcat might not be available.
# This will be a separate HEALTHCHECK layer if both are running.
# Docker combines HEALTHCHECKs if multiple are defined; the last one is used.
# To check both, a wrapper script is typically needed.
# For this iteration, let's assume we want to check one primary service determined by CMD.
# If CMD is `python /app/src/fastflight/server.py`, then Flight server health check is more relevant.
# If CMD is `python -m src.fastflight.cli start-all`, then both should be healthy.
# A single script for combined health check is better.

# For now, providing a combined health check script approach.
# Create a health check script first.

# Let's defer the complex combined health check script for a moment and set a primary CMD.
# The subtask implies refining the Dockerfile for the *Flight server* as per original CMD.
# However, the requirement also asks for FastAPI health check.
# This suggests the image should ideally run both via `start-all`.

# Default CMD - Changed to run both services using the CLI's start-all command.
# This makes both health checks potentially relevant.
CMD ["python", "-m", "src.fastflight.cli", "start-all"]

# Given the CMD runs `start-all`, a single HEALTHCHECK instruction
# should ideally verify both services. A wrapper script is best for this.
# For now, I will add a health check script and use it.
# This script will check FastAPI first, then Flight server.

COPY --chown=${APP_USER}:${APP_GROUP} docker/healthcheck.sh /app/healthcheck.sh
RUN chmod +x /app/healthcheck.sh

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD /app/healthcheck.sh
