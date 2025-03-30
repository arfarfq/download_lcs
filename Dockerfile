FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libcfitsio-dev \
    && rm -rf /var/lib/apt/lists/*

# Create directories with OpenShift-compatible permissions
RUN mkdir -p /mnt/data/TCEs_LCs && \
    chmod -R 777 /mnt/data /mnt/data/TCEs_LCs

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]