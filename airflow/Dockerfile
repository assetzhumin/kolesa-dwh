FROM apache/airflow:2.9.3-python3.11

# Install system packages as root
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates gcc g++ make \
    libc6-dev libpq-dev postgresql-client \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libxkbcommon0 \
    libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libgbm1 libasound2 \
    libpangocairo-1.0-0 libpango-1.0-0 libgtk-3-0 \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Ensure /opt/airflow is writable by airflow user (base image should handle this, but ensure it)
RUN chown -R airflow:root /opt/airflow && \
    chmod -R 775 /opt/airflow

# Switch to non-root user for application code
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Install browser (chromium only to save space/time)
# Playwright installs to user directory, so this works as non-root
RUN python -m playwright install chromium