FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libcfitsio-dev \
    && rm -rf /var/lib/apt/lists/*

# Create directories with proper permissions
RUN mkdir -p /mnt/data/TCEs_LCs && \
    chmod -R 777 /mnt/data /mnt/data/TCEs_LCs

WORKDIR /app

# First copy requirements to leverage Docker cache
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Then copy the rest of the application
COPY . .

CMD ["python", "download_tces.py"]