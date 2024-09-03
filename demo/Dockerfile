FROM python:3.12-slim as base

WORKDIR /app

COPY demo/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY demo .

EXPOSE 80
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]
