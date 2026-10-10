FROM apache/airflow:3.3.2-python3.14

USER airflow

# Install python requirements for DAGs and ETL pipelines
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
