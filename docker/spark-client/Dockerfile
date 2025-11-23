# Imagen base de Spark
FROM apache/spark:3.5.7

# Cambiar a usuario root para instalar dependencias
USER root

# Instalar dependencias del sistema
RUN apt-get update && \
    apt-get install -y python3-pip wget && \
    rm -rf /var/lib/apt/lists/*

# Crear directorio home para el usuario spark
RUN mkdir -p /home/spark && \
    chown spark:spark /home/spark

# Pre-descargar el driver JDBC de PostgreSQL para evitar descargas en tiempo de ejecución
RUN mkdir -p /opt/spark/jars && \
    wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar -O /opt/spark/jars/postgresql-42.7.3.jar && \
    chmod 644 /opt/spark/jars/postgresql-42.7.3.jar

# Copiar requirements.txt y instalar dependencias
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

# Configurar variables de entorno
ENV HOME=/root
ENV IVY_HOME=/root/.ivy2

# Establecer directorio de trabajo
WORKDIR /opt/spark/work-dir

# Asegurar que el directorio output tenga permisos correctos
RUN mkdir -p /opt/spark/work-dir/output && \
    chmod -R 777 /opt/spark/work-dir/output
