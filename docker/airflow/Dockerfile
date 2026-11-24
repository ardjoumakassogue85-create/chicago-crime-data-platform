# Image de base Airflow (bookworm)
FROM apache/airflow:2.10.5-python3.11

# Installer Java (nécessaire pour PySpark)
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        wget && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Copier les requirements et installer
COPY requirements.txt /tmp/requirements.txt
COPY requirements-dev.txt /tmp/requirements-dev.txt
# Indiquer la version d'Airflow empêche pip de la changer en installant tes dépendances
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt
RUN pip install --no-cache-dir pyspark==3.5.0

RUN mkdir -p /opt/airflow/dbt