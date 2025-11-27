FROM python:3.10-slim

WORKDIR /app

# Install git and system dependencies for pyqlib/tables
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    libhdf5-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Set environment variables to ensure output is flushed immediately
ENV PYTHONUNBUFFERED=1

# Run the scheduler
CMD ["python", "scheduler.py"]
