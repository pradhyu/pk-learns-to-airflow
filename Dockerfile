FROM apache/airflow:2.10.2-python3.11

USER root
# Install system packages if necessary
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       curl \
       sqlite3 \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install python requirements for DAGs and ETL pipelines
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
