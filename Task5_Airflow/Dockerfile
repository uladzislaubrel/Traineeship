FROM apache/airflow:2.11.0-python3.10

USER root

RUN python -m pip install --no-cache-dir \
    apache-airflow-providers-mongo \
    pymongo \
    pandas

USER airflow