FROM python:3.11-slim

# Install system dependencies for pymssql (FreeTDS)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    freetds-dev \
    freetds-bin \
    && rm -rf /var/lib/apt/lists/*

# Configure FreeTDS to use TLS 1.2 for Azure SQL
RUN echo "[global]" > /etc/freetds/freetds.conf && \
    echo "tds version = 7.4" >> /etc/freetds/freetds.conf && \
    echo "encryption = required" >> /etc/freetds/freetds.conf && \
    echo "openssl conf file = /etc/ssl/openssl.cnf" >> /etc/freetds/freetds.conf

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY code/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY code/ ./code/

# Create necessary directories
RUN mkdir -p data/sites/jsonl

# Expose port for API server (used by master)
EXPOSE 5001

# Set environment variables for production
ENV PYTHONUNBUFFERED=1

# Default command (can be overridden in k8s deployment)
CMD ["python3", "code/core/worker.py"]
