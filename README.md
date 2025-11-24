# 🚀 Pipeline de Análisis de Ventas con Prefect

Sistema de análisis de ventas distribuido que utiliza **Prefect** para orquestar pipelines de procesamiento con Apache Spark, generando métricas ejecutivas, análisis temporal, segmentación de clientes y recomendaciones de productos.

> **Nota**: Este proyecto ha sido migrado de Apache Airflow a Prefect para mejorar la flexibilidad, observabilidad y facilidad de uso. Ver [PREFECT_GUIDE.md](PREFECT_GUIDE.md) para más detalles.

## 📋 Tabla de Contenidos

- [Requisitos](#requisitos)
- [Inicio Rápido con Prefect](#inicio-rápido-con-prefect)
- [Pipelines Disponibles](#pipelines-disponibles)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Configuración Avanzada](#configuración-avanzada)
- [Guía de Prefect](#guía-de-prefect)
- [Troubleshooting](#troubleshooting)

## 🔧 Requisitos

- **Docker** y **Docker Compose** instalados
- **Git** (para clonar el repositorio)
- Al menos **8 GB de RAM** disponibles para los contenedores
- **Puerto 4200** libre para Prefect UI
- **Puerto 5432** libre para PostgreSQL

## ⚡ Inicio Rápido con Prefect

### Paso 1: Levantar los servicios

Desde el directorio `airflow/`:

```bash
docker compose up -d
```

Esto iniciará:

- **PostgreSQL** (base de datos)
- **Prefect Server** (orquestador - UI en puerto 4200)
- **Prefect Worker** (ejecutor de flows)
- **Spark Master y Worker** (procesamiento distribuido)

**Tiempo estimado**: 2-3 minutos

Verifica que todos los servicios estén corriendo:

```bash
docker compose ps
```

### Paso 1.5: Configurar Prefect (primera vez)

Después de levantar los servicios por primera vez, configura Prefect:

#### En Linux/Mac:

```bash
./scripts/linux/setup_prefect.sh
```

#### En Windows:

```cmd
scripts\windows\setup_prefect.bat
```

Esto creará el work pool necesario. Accede a la UI de Prefect en: **http://localhost:4200**

### Paso 2: Cargar los datos

Ahora puedes cargar los datos usando el **flow de Prefect** (automatizado) o los scripts tradicionales.

#### Opción A: Usando Prefect Flow (Recomendado)

##### En Linux/Mac:

```bash
./scripts/linux/run_prefect_flow.sh data_loading
```

##### En Windows:

```cmd
scripts\windows\run_prefect_flow.bat data_loading
```

#### Opción B: Scripts tradicionales

##### En Linux/Mac:

```bash
./scripts/linux/load_data.sh
```

##### En Windows:

```cmd
scripts\windows\load_data.bat
```

**Nota**: Los archivos CSV deben estar en el directorio `data/` con la estructura:

- `data/products/Categories.csv`
- `data/products/ProductCategory.csv`
- `data/transactions/*.csv`

**Tiempo estimado**: 5-10 minutos (depende del tamaño de los datos)

### Paso 3: Ejecutar los pipelines

Una vez cargados los datos, ejecuta los pipelines de análisis usando Prefect.

#### Ejecutar todos los pipelines (Flow Maestro)

Este flow ejecuta todos los pipelines en secuencia y sincroniza los resultados al frontend automáticamente.

##### En Linux/Mac:

```bash
./scripts/linux/run_prefect_flow.sh master
```

##### En Windows:

```cmd
scripts\windows\run_prefect_flow.bat master
```

#### Ejecutar pipelines individuales

##### En Linux/Mac:

```bash
./scripts/linux/run_prefect_flow.sh executive_summary
./scripts/linux/run_prefect_flow.sh analytics
./scripts/linux/run_prefect_flow.sh clustering
./scripts/linux/run_prefect_flow.sh recommendations
./scripts/linux/run_prefect_flow.sh output_sync  # Sincronizar al frontend
```

##### En Windows:

```cmd
scripts\windows\run_prefect_flow.bat executive_summary
scripts\windows\run_prefect_flow.bat analytics
scripts\windows\run_prefect_flow.bat clustering
scripts\windows\run_prefect_flow.bat recommendations
scripts\windows\run_prefect_flow.bat output_sync
```

**Tiempo estimado**:

- Resumen ejecutivo: 3-5 minutos
- Analítica temporal: 5-8 minutos
- Clustering: 8-12 minutos
- Recomendaciones: 10-15 minutos
- Flow maestro completo: 30-45 minutos

**Monitoreo**: Accede a http://localhost:4200 para ver el progreso en tiempo real

### Paso 4: Visualizar resultados

Los resultados se generan en formato JSON en el directorio `output/` y se sincronizan automáticamente al frontend si ejecutaste el `master_flow` o el `output_sync_flow`.

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

**Sincronización automática**: Si ejecutaste el flow maestro o el `output_sync_flow`, los archivos ya están en `../sales-frontend/public/data/`

**Para visualizar**: Ejecuta el dashboard React desde el directorio `sales-frontend/`

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
├── docker-compose.yml           # Orquestación de servicios (incluye Prefect)
├── requirements.txt             # Dependencias Python (incluye Prefect)
├── env.template                 # Template de variables de entorno
├── PREFECT_GUIDE.md             # Guía detallada de Prefect
│
├── src/                         # Código fuente
│   ├── run_pipeline.py          # CLI para ejecutar pipelines (legacy)
│   ├── prefect_config.py        # Configuración de Prefect
│   │
│   ├── config/
│   │   └── spark_config.py      # Configuración de Spark
│   │
│   ├── flows/                   # 🆕 Flows de Prefect
│   │   ├── data_loading_flow.py           # Flow de carga de datos
│   │   ├── executive_summary_flow.py      # Flow resumen ejecutivo
│   │   ├── analytics_flow.py              # Flow análisis temporal
│   │   ├── clustering_flow.py             # Flow clustering
│   │   ├── recommendations_flow.py        # Flow recomendaciones
│   │   ├── output_sync_flow.py            # Flow sincronización frontend
│   │   ├── master_flow.py                 # Flow maestro (orquesta todo)
│   │   └── notifications.py               # Sistema de notificaciones
│   │
│   ├── pipelines/               # Pipelines principales (lógica de negocio)
│   │   ├── executive_summary_pipeline.py
│   │   ├── analytics_pipeline.py
│   │   ├── clustering_pipeline.py
│   │   └── recommendations_pipeline.py
│   │
│   ├── analyzers/               # Módulos de análisis
│   │   ├── summary_metrics.py
│   │   ├── temporal_analyzer.py
│   │   ├── customer_analyzer.py
│   │   └── product_analyzer.py
│   │
│   ├── data_loader.py           # Carga de datos desde PostgreSQL
│   └── json_exporter.py         # Exportación de resultados
│
├── scripts/                     # Scripts de utilidad
│   ├── linux/
│   │   ├── load_data.sh         # Carga de datos (Linux/Mac)
│   │   ├── setup_prefect.sh     # 🆕 Configurar Prefect
│   │   └── run_prefect_flow.sh  # 🆕 Ejecutar flows de Prefect
│   └── windows/
│       ├── load_data.bat        # Carga de datos (Windows)
│       ├── setup_prefect.bat    # 🆕 Configurar Prefect
│       └── run_prefect_flow.bat # 🆕 Ejecutar flows de Prefect
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
│   ├── prefect-worker/Dockerfile  # 🆕 Dockerfile para Prefect Worker
│   ├── spark-client/Dockerfile
│   ├── spark-worker/Dockerfile
│   └── postgres/init-sales-db.sh
│
└── logs/                        # Logs y ejecuciones
    └── prefect_runs/            # 🆕 Logs de flows de Prefect
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

# Prefect
PREFECT_API_URL=http://prefect-server:4200/api

# Spark
SPARK_MASTER_URL=spark://spark-master:7077

# Paths
DATA_PATH=/opt/prefect/work-dir/data
OUTPUT_PATH=/opt/prefect/work-dir/output
FRONTEND_PATH=/opt/prefect/work-dir/frontend
```

### Acceso a Interfaces Web

Una vez levantados los servicios:

- **Prefect UI**: http://localhost:4200
  - Dashboard de flows, ejecuciones, logs y monitoreo
- **Spark Master UI**: http://localhost:8082
  - Monitoreo de jobs de Spark

### Configuración de Spark

Edita `src/config/spark_config.py` para ajustar:

- Memoria del driver y executors
- Número de cores
- Particiones de shuffle

### Parámetros de Pipelines con Prefect

Los flows de Prefect aceptan parámetros. Aunque actualmente se ejecutan con valores por defecto desde los scripts, puedes modificarlos en el código de los flows o crear deployments personalizados.

**Valores por defecto:**

- Clustering: `n_clusters=4`
- Recomendaciones: `min_support=0.005`, `min_confidence=0.2`
- Master Flow: ejecuta todos con los valores por defecto y sincroniza al frontend

Ver [PREFECT_GUIDE.md](PREFECT_GUIDE.md) para personalización avanzada.

## 🛠️ Comandos Útiles

### Gestión de servicios

```bash
# Iniciar servicios
docker compose up -d

# Detener servicios
docker compose down

# Ver logs en tiempo real (todos los servicios)
docker compose logs -f

# Ver logs de Prefect
docker compose logs -f prefect-server prefect-worker

# Reiniciar un servicio específico
docker compose restart prefect-worker

# Limpiar todo (incluyendo volúmenes)
docker compose down -v
```

### Acceso a contenedores

```bash
# Acceder a shell de Prefect Worker
docker compose exec prefect-worker bash

# Acceder a PostgreSQL
docker compose exec postgres psql -U sales -d sales

# Ejecutar flow manualmente desde contenedor
docker compose exec prefect-worker python -m flows.master_flow
```

### Verificación de datos

```bash
# Contar transacciones
docker compose exec postgres psql -U sales -d sales -c "SELECT COUNT(*) FROM transactions;"

# Ver categorías
docker compose exec postgres psql -U sales -d sales -c "SELECT * FROM categories LIMIT 10;"

# Verificar archivos generados
ls -lh output/summary/

# Verificar sincronización al frontend
ls -lh ../sales-frontend/public/data/
```

## 📘 Guía de Prefect

### ¿Por qué Prefect?

Prefect proporciona:

- **UI web moderna** para monitoreo en tiempo real
- **Recuperación automática** de fallos con retries configurables
- **Logging centralizado** con trazabilidad completa
- **Orquestación flexible** de flujos complejos
- **Ejecución manual o programada** según necesidad

### Flows Disponibles

1. **`data_loading_flow`**: Carga datos CSV a PostgreSQL automáticamente
2. **`executive_summary_flow`**: Genera métricas ejecutivas
3. **`analytics_flow`**: Análisis temporal y correlaciones
4. **`clustering_flow`**: Segmentación de clientes con K-Means
5. **`recommendations_flow`**: Sistema de recomendaciones con FP-Growth
6. **`output_sync_flow`**: Sincroniza outputs JSON al frontend
7. **`master_flow`**: Ejecuta todos los pipelines en secuencia y sincroniza

### Monitoreo

Accede a http://localhost:4200 para:

- Ver ejecuciones en progreso y completadas
- Inspeccionar logs detallados por task
- Revisar duración y rendimiento
- Consultar errores y stack traces
- Visualizar el grafo de dependencias de tasks

### Documentación Completa

Ver [PREFECT_GUIDE.md](PREFECT_GUIDE.md) para:

- Arquitectura detallada
- Cómo crear nuevos flows
- Configuración de schedules
- Troubleshooting avanzado
- Mejores prácticas

## 📝 Notas Importantes

- **Tiempo de procesamiento**: Los pipelines pueden tardar varios minutos dependiendo del tamaño de los datos y recursos disponibles
- **Persistencia**: Los datos en PostgreSQL persisten entre reinicios gracias a volúmenes Docker
- **Logs**: Se almacenan en `airflow/logs/` y persisten entre reinicios
- **Recursos**: Se recomienda al menos 8 GB de RAM para ejecutar todos los servicios simultáneamente
- **Resultados**: Los JSON generados están optimizados para consumo desde el frontend React

## 👥 Autores

- Juan David Colonia Aldana - A00395956
- Miguel Ángel Gonzalez Arango - A00395687
