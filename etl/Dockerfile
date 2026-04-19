FROM apache/airflow:latest

USER root

RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    python3-gdal \
    libproj-dev \
    proj-data \
    proj-bin \
    libgeos-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

RUN mkdir -p /opt/airflow/data /opt/airflow/logs /opt/airflow/models
RUN chmod -R 777 /opt/airflow/data /opt/airflow/logs /opt/airflow/models

COPY ./dags /opt/airflow/dags
COPY ./scripts /opt/airflow/scripts
COPY ./scripts/extract_script /opt/airflow/scripts/extract_script

WORKDIR /opt/airflow

CMD ["standalone"]
