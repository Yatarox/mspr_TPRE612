FROM apache/airflow:latest

USER root

# Dépendances système nécessaires à geopandas
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

# Installer les dépendances Python
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Crée les dossiers nécessaires
RUN mkdir -p /opt/airflow/data /opt/airflow/logs

# Droits
RUN chmod -R 777 /opt/airflow/data /opt/airflow/logs

# DAGs & scripts
COPY ./dags /opt/airflow/dags
COPY ./scripts /opt/airflow/scripts

WORKDIR /opt/airflow

CMD ["standalone"]
