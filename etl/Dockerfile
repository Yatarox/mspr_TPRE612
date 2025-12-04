FROM apache/airflow:latest

# Crée les dossiers nécessaires à l'intérieur du conteneur
RUN mkdir -p /opt/airflow/data /opt/airflow/logs

# Définit les droits corrects
RUN chmod -R 777 /opt/airflow/data /opt/airflow/logs

# Copie tes DAGs et scripts
COPY ./dags /opt/airflow/dags
COPY ./scripts /opt/airflow/scripts

# Définit le répertoire de travail
WORKDIR /opt/airflow

# Commande par défaut
CMD ["standalone"]
