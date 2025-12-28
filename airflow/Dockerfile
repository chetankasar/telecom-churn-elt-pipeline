# ============================================
# Apache Airflow Dockerfile
# ============================================
FROM apache/airflow:2.8.1-python3.11

# Set user to root for installing packages
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy and install Python requirements
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set working directory
WORKDIR /opt/airflow

