FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    default-libmysqlclient-dev \
    build-essential \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY config/ ./config/
COPY main.py .

RUN mkdir -p logs

EXPOSE 5001 5002 5003

CMD ["python", "main.py", "--node-id", "1"]
