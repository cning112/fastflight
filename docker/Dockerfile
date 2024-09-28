FROM python:3.10-buster

ENV PYTHONPATH="/app"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app
COPY src /app

CMD ["python", "/app/fastflight/flight_server.py"]
