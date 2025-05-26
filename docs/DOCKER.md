# Docker Deployment Guide

## Quick Start

```bash
# Run both servers (development)
docker-compose --profile dev up

# Run separated services (production-like)
docker-compose up
```

## Services

### `fastflight-server`
- **Port**: 8815 (gRPC)
- **Purpose**: Arrow Flight server for high-performance data streaming
- **Health Check**: Python import test

### `fastapi-server` 
- **Port**: 8000 (HTTP)
- **Purpose**: REST API gateway for Flight server
- **Dependencies**: Waits for Flight server to be healthy
- **Health Check**: GET `/fastflight/registered_data_types`

### `fastflight-dev`
- **Ports**: 8000 + 8815
- **Purpose**: Both servers in single container
- **Profile**: `dev` (use `--profile dev`)

## Commands

```bash
# Development setup
docker-compose --profile dev up

# Production-like (separated services)  
docker-compose up

# Scale Flight servers
docker-compose up --scale fastflight-server=3

# Build and run
docker-compose up --build

# Background mode
docker-compose up -d
```

## Manual Docker Build

```bash
# Build image
./scripts/build-docker.sh

# Build and push to ECR
./scripts/build-docker.sh --push

# Custom tag
./scripts/build-docker.sh --tag v1.0.0
```

## Container Usage

```bash
# Flight server only
docker run -p 8815:8815 fastflight:latest

# FastAPI server only  
docker run -p 8000:8000 fastflight:latest start-fastapi

# Both servers
docker run -p 8000:8000 -p 8815:8815 fastflight:latest start-all
```

## Environment Variables

- `FASTFLIGHT_HOST`: Server host (default: 0.0.0.0)
- `FASTFLIGHT_PORT`: Flight server port (default: 8815)
- `PYTHONPATH`: Set to `/app/src` in container

## Health Checks

Services include health checks for proper startup ordering and monitoring:
- Flight server: Python import validation
- FastAPI server: REST endpoint availability
