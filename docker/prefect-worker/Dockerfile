# Imagen base de Python (bookworm tiene mejor soporte para OpenJDK 17)
FROM python:3.11-slim-bookworm

# Cambiar a usuario root para instalar dependencias
USER root

# Instalar dependencias del sistema necesarias
RUN apt-get update && \
    apt-get install -y \
    wget \
    curl \
    openjdk-17-jre-headless \
    procps \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Configurar Java home para PySpark (Java 17 para compatibilidad con Spark 3.5.x)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Crear directorio para JARs de Spark
RUN mkdir -p /opt/spark/jars

# Descargar el driver JDBC de PostgreSQL
RUN wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
    -O /opt/spark/jars/postgresql-42.7.3.jar && \
    chmod 644 /opt/spark/jars/postgresql-42.7.3.jar

# Configurar variables de entorno para Spark
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PYSPARK_SUBMIT_ARGS="--jars /opt/spark/jars/postgresql-42.7.3.jar pyspark-shell"

# Crear directorio de trabajo
WORKDIR /opt/prefect

# Copiar requirements.txt
COPY requirements.txt /opt/prefect/requirements.txt

# Instalar dependencias de Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Crear directorios necesarios
RUN mkdir -p /opt/prefect/work-dir/output && \
    mkdir -p /opt/prefect/work-dir/logs && \
    mkdir -p /opt/prefect/work-dir/logs/prefect_runs && \
    chmod -R 777 /opt/prefect/work-dir

# Configurar PYTHONPATH
ENV PYTHONPATH=/opt/prefect/work-dir/src

# Comando por defecto (se sobrescribe en docker-compose)
CMD ["prefect", "worker", "start", "--pool", "default-pool", "--type", "process"]

