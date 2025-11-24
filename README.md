# 🚀 Pipeline de Análisis de Ventas con Apache Airflow

Sistema de análisis de ventas distribuido que utiliza Apache Airflow para orquestar pipelines de procesamiento con Apache Spark, generando métricas ejecutivas, análisis temporal, segmentación de clientes y recomendaciones de productos.

## 📋 Tabla de Contenidos

- [Requisitos](#requisitos)
- [Inicio Rápido](#inicio-rápido)
- [Pipelines Disponibles](#pipelines-disponibles)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Configuración Avanzada](#configuración-avanzada)
- [Troubleshooting](#troubleshooting)

## 🔧 Requisitos

- **Docker** y **Docker Compose** instalados
- **Git** (para clonar el repositorio)
- Al menos **8 GB de RAM** disponibles para los contenedores
- **Puerto 8085** libre para Airflow UI

## ⚡ Inicio Rápido

### Paso 1: Levantar los servicios

Desde el directorio `airflow/`:

```bash
docker compose up -d
```

Esto iniciará:

- PostgreSQL (base de datos)
- Apache Airflow (scheduler, webserver, worker)
- Spark Master y Worker (procesamiento distribuido)

**Tiempo estimado**: 2-3 minutos

Verifica que todos los servicios estén corriendo:

```bash
docker compose ps
```

### Paso 2: Cargar los datos

Una vez que los servicios estén activos, carga los datos en PostgreSQL.

#### En Linux/Mac:

```bash
./scripts/load_data.sh ../data
```

#### En Windows:

```cmd
scripts\windows\load_data.bat ..\data
```

**Nota**: Ajusta la ruta `../data` según la ubicación de tus archivos CSV (`Categories.csv`, `Product_Categories.csv`, `transactions/`).

**Tiempo estimado**: 5-10 minutos (depende del tamaño de los datos)

### Paso 3: Ejecutar los pipelines

Una vez cargados los datos, ejecuta los pipelines de análisis.

#### En Linux/Mac:

```bash
# Ejecutar todos los pipelines
./scripts/run_all_pipelines.sh

# O ejecutar pipelines individuales
cd src
python run_pipeline.py executive_summary
python run_pipeline.py analytics
python run_pipeline.py clustering --n-clusters 4
python run_pipeline.py recommendations --min-support 0.005 --min-confidence 0.2
```

#### En Windows:

```cmd
REM Ejecutar todos los pipelines
scripts\windows\run_pipeline_docker.bat all

REM O ejecutar pipelines individuales
scripts\windows\run_pipeline_docker.bat executive_summary
scripts\windows\run_pipeline_docker.bat analytics
scripts\windows\run_pipeline_docker.bat clustering
scripts\windows\run_pipeline_docker.bat recommendations
```

**Tiempo estimado**:

- Resumen ejecutivo: 3-5 minutos
- Analítica temporal: 5-8 minutos
- Clustering: 8-12 minutos
- Recomendaciones: 10-15 minutos

### Paso 4: Visualizar resultados

Los resultados se generan en formato JSON en el directorio `output/`:

```
output/
├── summary/              # Métricas ejecutivas
│   ├── basic_metrics.json
│   ├── top_10_products.json
│   └── top_10_customers.json
├── analytics/            # Series temporales y correlaciones
│   ├── daily_sales.json
│   └── variable_correlation.json
├── advanced/
│   └── clustering/       # Segmentación de clientes
│       ├── cluster_summary.json
│       └── clustering_visualization.json
└── recommendations/      # Recomendaciones de productos
    ├── product_recs.json
    └── customer_recs.json
```

**Para visualizar en el frontend**: Copia los archivos JSON a `sales-frontend/public/data/` y ejecuta el dashboard React.

## 📊 Pipelines Disponibles

### 1. Resumen Ejecutivo (`executive_summary`)

Genera métricas clave del negocio:

- Total de transacciones y productos vendidos
- Top 10 productos más vendidos
- Top 10 clientes más activos
- Top 10 categorías por volumen
- Días pico de ventas

**Salida**: `output/summary/*.json`

### 2. Analítica Temporal (`analytics`)

Análisis de patrones temporales y correlaciones:

- Series de tiempo (diarias, semanales, mensuales)
- Patrones por día de la semana
- Distribución de productos por categoría y tienda (boxplot)
- Matriz de correlación entre variables

**Salida**: `output/analytics/*.json`

### 3. Segmentación de Clientes (`clustering`)

Clustering K-Means para identificar perfiles de clientes:

- 4 clusters: VIP/Premium, Exploradores, Ocasionales, Nuevos
- Métricas por cluster (frecuencia, volumen, diversidad)
- Recomendaciones de negocio por segmento
- Visualización de clasificación (scatter plot)

**Parámetros**:

- `--n-clusters`: Número de clusters (default: 4)

**Salida**: `output/advanced/clustering/*.json`

### 4. Recomendaciones (`recommendations`)

Sistema de recomendaciones basado en reglas de asociación (FP-Growth):

- **Por producto**: Productos complementarios que suelen comprarse juntos
- **Por cliente**: Sugerencias personalizadas según historial de compra

**Parámetros**:

- `--min-support`: Soporte mínimo para reglas (default: 0.005)
- `--min-confidence`: Confianza mínima para reglas (default: 0.2)

**Salida**: `output/recommendations/*.json`

## 📁 Estructura del Proyecto

```
airflow/
├── docker-compose.yml           # Orquestación de servicios
├── requirements.txt             # Dependencias Python
├── env.template                 # Template de variables de entorno
│
├── src/                         # Código fuente
│   ├── run_pipeline.py          # CLI para ejecutar pipelines
│   ├── config/
│   │   └── spark_config.py      # Configuración de Spark
│   ├── pipelines/               # Pipelines principales
│   │   ├── executive_summary_pipeline.py
│   │   ├── analytics_pipeline.py
│   │   ├── clustering_pipeline.py
│   │   └── recommendations_pipeline.py
│   ├── analyzers/               # Módulos de análisis
│   │   ├── summary_metrics.py
│   │   ├── temporal_analyzer.py
│   │   ├── customer_analyzer.py
│   │   └── product_analyzer.py
│   ├── data_loader.py           # Carga de datos desde PostgreSQL
│   └── json_exporter.py         # Exportación de resultados
│
├── scripts/                     # Scripts de utilidad
│   ├── load_data.sh             # Carga de datos (Linux/Mac)
│   └── windows/
│       ├── load_data.bat        # Carga de datos (Windows)
│       └── run_pipeline_docker.bat  # Ejecutar pipelines (Windows)
│
├── data/                        # Datos CSV (montar aquí)
│   ├── Categories.csv
│   ├── Product_Categories.csv
│   └── transactions/
│
├── output/                      # Resultados generados
│   ├── summary/
│   ├── analytics/
│   ├── advanced/
│   └── recommendations/
│
├── docker/                      # Configuración Docker
│   ├── airflow/Dockerfile
│   └── postgres/init-sales-db.sh
│
└── logs/                        # Logs de Airflow
```

## ⚙️ Configuración Avanzada

### Variables de Entorno

Crea un archivo `.env` basado en `env.template`:

```bash
cp env.template .env
```

**Variables principales**:

```bash
# PostgreSQL
POSTGRES_USER=sales
POSTGRES_PASSWORD=sales
POSTGRES_DB=sales

# Airflow
AIRFLOW_UID=50000
AIRFLOW_FERNET_KEY=<generar con: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())">

# Spark
SPARK_MASTER_URL=spark://spark-master:7077
```

### Acceso a Interfaces Web

Una vez levantados los servicios:

- **Airflow UI**: http://localhost:8085
  - Usuario: `admin`
  - Contraseña: `admin`
- **Spark Master UI**: http://localhost:8080

### Configuración de Spark

Edita `src/config/spark_config.py` para ajustar:

- Memoria del driver y executors
- Número de cores
- Particiones de shuffle

### Parámetros de Pipelines

#### Clustering

```bash
python run_pipeline.py clustering --n-clusters 5
```

#### Recomendaciones

```bash
python run_pipeline.py recommendations \
  --min-support 0.01 \
  --min-confidence 0.3
```

## 🛠️ Comandos Útiles

### Gestión de servicios

```bash
# Iniciar servicios
docker compose up -d

# Detener servicios
docker compose down

# Ver logs en tiempo real
docker compose logs -f

# Reiniciar un servicio específico
docker compose restart airflow-scheduler

# Limpiar todo (incluyendo volúmenes)
docker compose down -v
```

### Acceso a contenedores

```bash
# Acceder a shell de Airflow
docker compose exec airflow-scheduler bash

# Acceder a PostgreSQL
docker compose exec postgres psql -U sales -d sales

# Ejecutar comando Python en Airflow
docker compose exec airflow-scheduler python /opt/airflow/src/run_pipeline.py --help
```

### Verificación de datos

```bash
# Contar transacciones
docker compose exec postgres psql -U sales -d sales -c "SELECT COUNT(*) FROM transactions;"

# Ver categorías
docker compose exec postgres psql -U sales -d sales -c "SELECT * FROM categories LIMIT 10;"

# Verificar archivos generados
ls -lh output/summary/
```

## 📝 Notas Importantes

- **Tiempo de procesamiento**: Los pipelines pueden tardar varios minutos dependiendo del tamaño de los datos y recursos disponibles
- **Persistencia**: Los datos en PostgreSQL persisten entre reinicios gracias a volúmenes Docker
- **Logs**: Se almacenan en `airflow/logs/` y persisten entre reinicios
- **Recursos**: Se recomienda al menos 8 GB de RAM para ejecutar todos los servicios simultáneamente
- **Resultados**: Los JSON generados están optimizados para consumo desde el frontend React

## 👥 Autores

- Juan David Colonia Aldana - A00395956
- Miguel Ángel Gonzalez Arango - A00395687
