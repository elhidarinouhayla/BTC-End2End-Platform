FROM apache/airflow:2.8.1

USER root

# Install Java 
RUN apt-get update \
    && apt-get install -y openjdk-17-jdk wget \
    && apt-get clean

# Télécharger le driver PostgreSQL JDBC pour PySpark
RUN mkdir -p /opt/airflow/jars && \
    wget https://jdbc.postgresql.org/download/postgresql-42.7.1.jar -O /opt/airflow/jars/postgresql-42.7.1.jar

# Configurer les variables d'environnement
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
ENV PYSPARK_SUBMIT_ARGS="--jars /opt/airflow/jars/postgresql-42.7.1.jar pyspark-shell"

USER airflow

# Install PySpark separately with increased timeout and retries
RUN pip install --no-cache-dir --timeout=1000 --retries=5 pyspark==3.5.0

# Install remaining dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --timeout=600 -r /requirements.txt