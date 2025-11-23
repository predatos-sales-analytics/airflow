# Imagen base de Spark
FROM apache/spark:3.5.7

# Cambiar a usuario root para instalar dependencias
USER root

# Instalar wget para descargar el driver JDBC
RUN apt-get update && \
    apt-get install -y wget && \
    rm -rf /var/lib/apt/lists/*

# Descargar el driver JDBC de PostgreSQL
RUN mkdir -p /opt/spark/jars && \
    wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar -O /opt/spark/jars/postgresql-42.7.3.jar && \
    chmod 644 /opt/spark/jars/postgresql-42.7.3.jar
