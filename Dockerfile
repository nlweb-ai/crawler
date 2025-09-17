# Use Python 3.10 slim image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire application
COPY . .

# Initialize git submodule (nlweb-submodule)
RUN git submodule update --init --recursive || echo "Submodule initialization skipped"

# Create necessary directories
RUN mkdir -p data/docs data/embeddings data/json data/json_with_embeddings data/keys data/status data/urls logs

# Set environment variables
ENV PYTHONPATH=/app:/app/code:/app/nlweb-submodule/code/python
ENV FLASK_APP=run.py
ENV FLASK_ENV=production
ENV NLWEB_CONFIG_DIR=/app/config

# Expose the Flask port
EXPOSE 5000

# Run the application
CMD ["python", "run.py"]
