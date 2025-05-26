#!/bin/sh
set -eo pipefail

# Default ports, can be overridden by environment variables if needed by the script
FASTAPI_PORT="${FASTFLIGHT_API_PORT:-8000}"
FLIGHT_PORT="${FASTFLIGHT_SERVER_PORT:-8815}"
FASTAPI_HOST="${FASTFLIGHT_API_HOST:-localhost}" # Healthcheck runs inside the container
FLIGHT_HOST="${FASTFLIGHT_SERVER_HOST:-localhost}" # Healthcheck runs inside the container

echo "Healthcheck: Checking FastAPI server at http://${FASTAPI_HOST}:${FASTAPI_PORT}/fastflight/health"
if curl -fsS "http://${FASTAPI_HOST}:${FASTAPI_PORT}/fastflight/health" > /dev/null; then
  echo "Healthcheck: FastAPI server is healthy."
else
  echo "Healthcheck: FastAPI server failed."
  exit 1
fi

echo "Healthcheck: Checking Flight server TCP connection at ${FLIGHT_HOST}:${FLIGHT_PORT}"
# Use python to do a simple TCP check for the Flight server
# This avoids needing netcat or other tools not guaranteed in slim images.
# The python in /opt/venv/bin should be available.
if /opt/venv/bin/python -c "import socket; s = socket.socket(socket.AF_INET, socket.SOCK_STREAM); s.settimeout(5); s.connect(('${FLIGHT_HOST}', ${FLIGHT_PORT})); s.close()"; then
  echo "Healthcheck: Flight server TCP connection successful."
else
  echo "Healthcheck: Flight server TCP connection failed."
  exit 1
fi

echo "Healthcheck: All services healthy."
exit 0
