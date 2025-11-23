# 🚀 Pipeline de Análisis de Ventas con Apache Airflow

Pipeline de análisis de ventas orquestado con Apache Airflow, ejecutando análisis distribuidos con Apache Spark y procesamiento paralelo por tienda.

## 📋 Tabla de Contenidos

- [Requisitos](#requisitos)
- [Instalación](#instalación)
- [Configuración](#configuración)
- [Uso](#uso)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [DAGs Disponibles](#dags-disponibles)
- [Carga de Datos](#carga-de-datos)
- [Troubleshooting](#troubleshooting)

## 🔧 Requisitos

- Docker y Docker Compose
- Git (para clonar este repositorio)

## 📦 Instalación

### 1. Configurar variables de entorno

```bash
# Copiar template de variables de entorno
cp env.template .env

# Editar .env con tus valores
# Generar AIRFLOW_FERNET_KEY si no existe:
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 3. Inicializar Airflow

```bash
# Crear usuario y base de datos de Airflow
docker compose up airflow-init

# Esperar a que termine (verás "User 'admin' created" al finalizar)
```

### 4. Levantar servicios

```bash
# Iniciar todos los servicios
docker compose up -d

# Ver logs
docker compose logs -f
```

## ⚙️ Configuración

### Variables de Entorno (.env)

El archivo `.env` contiene todas las configuraciones necesarias. Ver `.env.template` para referencia.

**Variables críticas:**

- `AIRFLOW_UID`: ID de usuario para Airflow (generado automáticamente en Linux/Mac)
- `AIRFLOW_FERNET_KEY`: Clave de encriptación para Airflow (generar con el comando de instalación)

### Variables de Airflow

Configurar en la UI de Airflow (Admin → Variables) o vía CLI:

```bash
# Tamaño de muestra para transacciones (opcional, None = todas)
airflow variables set sales_transactions_sample_size 100000

# Parámetros FP-Growth
airflow variables set sales_fp_growth_min_support 0.05
airflow variables set sales_fp_growth_min_confidence 0.4
```

### Conexiones de Airflow

La conexión `sales_postgres` se crea automáticamente durante `airflow-init`. Si necesitas modificarla:

```bash
airflow connections add 'sales_postgres' \
  --conn-uri 'postgresql://sales:sales@postgres:5432/sales'
```

## 🚀 Uso

### Acceder a la UI de Airflow

1. Abrir navegador en: http://localhost:8085
2. Credenciales por defecto:
   - Usuario: `admin`
   - Contraseña: `admin`

### Ejecutar DAGs

1. En la UI, ir a **DAGs**
2. Activar el DAG deseado (toggle ON/OFF)
3. Hacer clic en **Trigger DAG** (▶️) para ejecución manual

### Orden recomendado de ejecución

1. **Cargar datos** (ver sección [Carga de Datos](#carga-de-datos))
2. `categories_reference_pipeline` - Análisis de categorías
3. `transactions_quality_pipeline` - Análisis de calidad de transacciones
4. `advanced_sales_analytics` - Análisis avanzados (temporal, clientes, productos, FP-Growth)

### Monitoreo

- **Logs de tareas**: Click en la tarea → Logs
- **Grafana/Spark UI**: http://localhost:8080 (Spark Master)
- **Postgres**: `localhost:5432` (usuario: `airflow` / `sales`)

## 📁 Estructura del Proyecto

```
airflow/
├── dags/                    # DAGs de Airflow
│   ├── categories_dag.py
│   ├── transactions_dag.py
│   └── advanced_analysis_dag.py
├── includes/                # Módulos compartidos
│   ├── bootstrap.py         # Inicialización de rutas
│   ├── pipeline_context.py  # Context manager para pipeline
│   ├── tasks.py             # Tareas reutilizables
│   └── store_service.py     # Servicio de consulta de tiendas
├── config/                  # Configuración de Spark
│   └── spark_config.py
├── src/                     # Módulos de análisis
│   ├── data_loader.py
│   ├── pipeline.py
│   ├── eda_analyzer.py
│   ├── visualizer.py
│   ├── utils.py
│   └── analyzers/
│       ├── customer_analyzer.py
│       ├── temporal_analyzer.py
│       └── product_analyzer.py
├── data/                    # Datos CSV (montados en contenedor)
│   ├── products/
│   └── transactions/
├── docker/                  # Configuración Docker
│   ├── airflow/
│   │   └── Dockerfile
│   └── postgres/
│       └── init-sales-db.sh
├── scripts/                 # Scripts de utilidad
│   ├── load_data.sh
│   └── load_data.bat
├── logs/                    # Logs de Airflow (volumen)
├── plugins/                 # Plugins personalizados
├── docker-compose.yml       # Orquestación de servicios
├── requirements.txt         # Dependencias Python
├── env.template             # Template de variables de entorno
└── README.md                # Este archivo
```

## 🔄 DAGs Disponibles

### 1. `categories_reference_pipeline`

**Descripción**: Analiza datasets de referencia (categorías y productos-categorías).

**Tareas**:

- `analyze_categories` → `analyze_product_categories`

**Duración estimada**: 1-2 minutos

### 2. `transactions_quality_pipeline`

**Descripción**: Ejecuta análisis de calidad y datasets explodidos de transacciones.

**Tareas**:

- `analyze_transactions` → `analyze_transactions_exploded`

**Duración estimada**: 5-10 minutos (depende del tamaño de datos)

### 3. `advanced_sales_analytics`

**Descripción**: Análisis completo con procesamiento paralelo por tienda y FP-Growth distribuido.

**Tareas**:

- `temporal_analysis` (paralelo)
- `customer_analysis` (paralelo)
- `global_product_analysis` (paralelo)
- `fetch_store_ids` → `analyze_store[store_1, store_2, ...]` (paralelo por tienda)
- `train_fp_growth` (distribuido en Spark cluster)

**Flujo**:

```
[temporal, customers, products]
    ↓
[analyze_store × N tiendas] (paralelo)
    ↓
train_fp_growth
```

**Duración estimada**: 15-30 minutos (depende del número de tiendas y tamaño de datos)

## 📥 Carga de Datos

### Requisitos previos

- Servicios de Docker Compose ejecutándose
- Base de datos `sales` creada (se crea automáticamente en `airflow-init`)

### Opción 1: Script Bash (Linux/Mac)

```bash
# Desde la raíz del proyecto airflow
./scripts/load_data.sh [ruta_a_data]

# Ejemplo con ruta relativa al proyecto principal
./scripts/load_data.sh ../product-sales-analytics/data
```

### Opción 2: Script Batch (Windows)

```cmd
REM Desde la raíz del proyecto airflow
scripts\load_data.bat [ruta_a_data]

REM Ejemplo
scripts\load_data.bat ..\product-sales-analytics\data
```

### Opción 3: Manual con psql

```bash
# Conectar al contenedor Postgres
docker compose exec postgres psql -U sales -d sales

# Ejecutar comandos COPY manualmente
\copy categories FROM '/path/to/Categories.csv' WITH (FORMAT csv, DELIMITER '|');
```

### Verificar carga

```bash
# Conectar y consultar
docker compose exec postgres psql -U sales -d sales -c "SELECT COUNT(*) FROM transactions;"
```

## 🔍 Troubleshooting

### Error: "ModuleNotFoundError: No module named 'config'"

**Causa**: Los módulos `config/` o `src/` no están disponibles en el contenedor.

**Solución**:

1. Verificar que las carpetas `config/` y `src/` existen en el directorio `airflow/`
2. Verificar que los volúmenes están montados correctamente en `docker-compose.yml`
3. Verificar que el volumen está montado correctamente en `docker-compose.yml`

### Error: "Connection refused" a Postgres

**Causa**: Postgres no está listo o las credenciales son incorrectas.

**Solución**:

```bash
# Verificar estado
docker compose ps postgres

# Ver logs
docker compose logs postgres

# Reiniciar servicio
docker compose restart postgres
```

### Error: "Spark master not available"

**Causa**: El clúster Spark no está iniciado o la URL es incorrecta.

**Solución**:

```bash
# Verificar servicios Spark
docker compose ps | grep spark

# Ver logs del master
docker compose logs spark-master

# Verificar URL en .env: SPARK_MASTER_URL=spark://spark-master:7077
```

### DAGs no aparecen en la UI

**Causa**: Errores de sintaxis o imports en los DAGs.

**Solución**:

```bash
# Verificar logs del scheduler
docker compose logs airflow-scheduler

# Validar DAGs
docker compose exec airflow-scheduler airflow dags list

# Ver errores específicos
docker compose exec airflow-scheduler airflow dags list-import-errors
```

### Permisos en volúmenes (Linux/Mac)

**Causa**: Problemas de permisos con el usuario de Airflow.

**Solución**:

```bash
# Ajustar permisos
sudo chown -R 50000:0 airflow/logs airflow/plugins

# O regenerar AIRFLOW_UID
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

## 🔗 Enlaces Útiles

- **Airflow UI**: http://localhost:8085
- **Spark Master UI**: http://localhost:8080
- **Documentación Airflow**: https://airflow.apache.org/docs/
- **Documentación Spark**: https://spark.apache.org/docs/latest/

## 📝 Notas

- Los resultados se guardan en `output/` dentro del proyecto principal (montado como volumen)
- Los análisis por tienda generan CSVs en `output/stores/<store_id>/`
- FP-Growth guarda resultados en `output/data/fp_growth_*`
- El pipeline de recomendaciones escribe JSON consumibles por el frontend en `output/recommendations/{product_recs,customer_recs}.json`
- Los logs de Airflow se almacenan en `airflow/logs/` (persisten entre reinicios)

## 👥 Autores

- Juan David Colonia Aldana - A00395956
- Miguel Ángel Gonzalez Arango - A00395687
