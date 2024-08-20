FROM python:3.12-slim as base

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config ./config
COPY src .

EXPOSE 80
CMD ["uvicorn", "fastflight.main:app", "--host", "0.0.0.0", "--port", "80", "--log-config", "config/log-config.yaml"]
