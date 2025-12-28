# ============================================
# Apache Superset Dockerfile
# ============================================
FROM apache/superset:3.1.0

# Set user to root for installing packages
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to superset user
USER superset

# Copy and install Python requirements
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy Superset configuration
COPY superset_config.py /app/pythonpath/superset_config.py

# Set working directory
WORKDIR /app

# Expose Superset port
EXPOSE 8088

