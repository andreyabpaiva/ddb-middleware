FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies for MySQL client
RUN apt-get update && apt-get install -y \
    default-libmysqlclient-dev \
    build-essential \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY config/ ./config/
COPY main.py .

# Create logs directory
RUN mkdir -p logs

# Expose ports for middleware nodes
EXPOSE 5001 5002 5003

# Default command (will be overridden in docker-compose)
CMD ["python", "main.py", "--node-id", "1"]
