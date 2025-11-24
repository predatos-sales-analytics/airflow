# 🚀 Pipeline de Análisis de Ventas con Prefect

Sistema de análisis de ventas distribuido que utiliza **Prefect** para orquestar pipelines de procesamiento con Apache Spark, generando métricas ejecutivas, análisis temporal, segmentación de clientes y recomendaciones de productos.

> **Nota**: Este proyecto ha sido migrado de Apache Airflow a Prefect para mejorar la flexibilidad, observabilidad y facilidad de uso. Ver [PREFECT_GUIDE.md](PREFECT_GUIDE.md) para más detalles.

## 📋 Tabla de Contenidos

- [Requisitos](#requisitos)
- [Inicio Rápido con Prefect](#inicio-rápido-con-prefect)
- [Pipelines Disponibles](#pipelines-disponibles)
- [Monitor de Nuevos Datos](#monitor-de-nuevos-datos)
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

Esto hará:

- ✅ Crear el work pool necesario
- ✅ Configurar el **monitor de datos automático** (se ejecuta cada 2 minutos)
- ✅ Dejar todo listo para ejecutar flows

Accede a la UI de Prefect en: **http://localhost:4200**

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

## 🔍 Monitor de Nuevos Datos

El sistema incluye un **monitor de datos automático** que detecta cuando hay nuevos datos en la base de datos PostgreSQL.

### ⚡ Configuración automática

**El monitor se activa automáticamente** al ejecutar `setup_prefect`. No necesitas configuración adicional:

- ✅ Se ejecuta **cada 2 minutos** automáticamente
- ✅ Detecta nuevas transacciones, productos o categorías
- ✅ Notifica cuando hay cambios
- ✅ Guarda el estado entre verificaciones

### Características

- Verificar si hay nuevas transacciones, productos o categorías
- Comparar el estado actual con la última verificación
- Notificar cuando se detectan cambios
- Gestión completa desde la UI de Prefect

### ¿Cómo funciona?

El monitor:

1. Consulta el estado actual de la base de datos (conteo de registros y fechas)
2. Compara con el último estado guardado en `output/metadata/data_monitor_state.json`
3. Detecta cambios y notifica
4. Guarda el nuevo estado para la próxima verificación

### Ejecutar el monitor manualmente

#### En Windows:

```cmd
scripts\windows\monitor_data.bat
```

#### En Linux/Mac:

```bash
./scripts/linux/monitor_data.sh
```

También puedes usar el script genérico de flows:

```bash
# Windows
scripts\windows\run_prefect_flow.bat data_monitor

# Linux/Mac
./scripts/linux/run_prefect_flow.sh data_monitor
```

### Ejemplo de salida

Cuando hay datos nuevos:

```
🔔 NUEVOS DATOS DETECTADOS EN LA BASE DE DATOS
======================================================================
📈 Nuevas transacciones: +1500 (Total: 50000)
💡 Sugerencia: Ejecuta el master flow para actualizar los análisis
   Comando: ./scripts/[windows|linux]/run_prefect_flow.[bat|sh] master
======================================================================
```

Cuando no hay cambios:

```
✅ Base de datos sin cambios desde la última verificación
```

### Configuración

El archivo `data_monitor_config.json` contiene la configuración del monitor:

```json
{
  "monitor": {
    "auto_trigger_master": false, // Disparar master flow automáticamente
    "save_state": true, // Guardar estado para próxima verificación
    "check_interval_minutes": 30 // Intervalo recomendado (para deployments)
  }
}
```

### Monitoreo automático (Deployment)

**¡El monitor se configura automáticamente!** Al ejecutar el script `setup_prefect`, se crea un deployment que ejecuta el monitor **cada 2 minutos** de manera automática.

#### Gestionar el deployment

Desde la UI de Prefect (http://localhost:4200/deployments):

- **Ver estado**: Navega a "Deployments" → "data-monitor-scheduled"
- **Pausar**: Click en "Pause" para detener la ejecución automática
- **Reanudar**: Click en "Resume" para reactivar el monitor
- **Ver historial**: Revisa todas las ejecuciones y sus resultados

El deployment está configurado con:

- ⏱️ **Intervalo**: Cada 2 minutos
- 🔔 **Notificaciones**: Solo cuando hay datos nuevos
- 💾 **Estado**: Se guarda automáticamente entre ejecuciones
- 🚀 **Auto-trigger master**: Deshabilitado (solo notifica)

#### Ejecución manual adicional

Si necesitas verificar manualmente sin esperar al próximo intervalo:

```bash
# Windows
scripts\windows\monitor_data.bat

# Linux/Mac
./scripts/linux/monitor_data.sh
```

### Monitoreo continuo alternativo (opcional)

Si prefieres usar herramientas del sistema operativo en lugar del deployment de Prefect:

**Opción A: Usar un cron job (Linux/Mac)**

```bash
# Verificar cada 30 minutos
*/30 * * * * cd /ruta/al/proyecto/airflow && ./scripts/linux/monitor_data.sh >> logs/monitor.log 2>&1
```

**Opción B: Usar Task Scheduler (Windows)**

1. Abre el Programador de tareas
2. Crea una nueva tarea básica
3. Configura para ejecutar `scripts\windows\monitor_data.bat` cada 30 minutos

**Nota**: Si usas estas opciones, considera pausar el deployment de Prefect para evitar ejecuciones duplicadas.

### Estado del monitor

El archivo de estado se guarda en:

```
output/metadata/data_monitor_state.json
```

Contiene:

- Número total de transacciones
- Fecha de la última transacción
- Conteos de categorías y productos
- Timestamp de la última verificación

Este archivo se actualiza automáticamente después de cada ejecución del monitor.

## 📁 Estructura del Proyecto

```
airflow/
├── docker-compose.yml           # Orquestación de servicios (incluye Prefect)
├── requirements.txt             # Dependencias Python (incluye Prefect)
├── env.template                 # Template de variables de entorno
├── PREFECT_GUIDE.md             # Guía detallada de Prefect
├── DATA_MONITOR_GUIDE.md        # 🆕 Guía del monitor de nuevos datos
│
├── src/                         # Código fuente
│   ├── run_pipeline.py          # CLI para ejecutar pipelines (legacy)
│   ├── prefect_config.py        # Configuración de Prefect
│   │
│   ├── config/
│   │   └── spark_config.py      # Configuración de Spark
│   │
│   ├── deployments/             # 🆕 Deployments de Prefect
│   │   └── monitor_deployment.py          # Deployment automático del monitor
│   │
│   ├── flows/                   # 🆕 Flows de Prefect
│   │   ├── data_loading_flow.py           # Flow de carga de datos
│   │   ├── data_monitor_flow.py           # Flow de monitoreo de nuevos datos
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
│   │   ├── run_prefect_flow.sh  # 🆕 Ejecutar flows de Prefect
│   │   └── monitor_data.sh      # 🆕 Monitorear nuevos datos
│   └── windows/
│       ├── load_data.bat        # Carga de datos (Windows)
│       ├── setup_prefect.bat    # 🆕 Configurar Prefect
│       ├── run_prefect_flow.bat # 🆕 Ejecutar flows de Prefect
│       └── monitor_data.bat     # 🆕 Monitorear nuevos datos
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
│   ├── recommendations/
│   └── metadata/                # 🆕 Estado del monitor y metadata
│       └── data_monitor_state.json
│
├── docker/                      # Configuración Docker
│   ├── prefect-worker/Dockerfile  # 🆕 Dockerfile para Prefect Worker
│   ├── spark-client/Dockerfile
│   ├── spark-worker/Dockerfile
│   └── postgres/init-sales-db.sh
│
├── data_monitor_config.json     # 🆕 Configuración del monitor de datos
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
2. **`data_monitor_flow`**: 🆕 Monitorea y detecta nuevos datos en la base de datos
   - 🤖 **Deployment automático**: Se ejecuta cada 2 minutos al configurar Prefect
3. **`executive_summary_flow`**: Genera métricas ejecutivas
4. **`analytics_flow`**: Análisis temporal y correlaciones
5. **`clustering_flow`**: Segmentación de clientes con K-Means
6. **`recommendations_flow`**: Sistema de recomendaciones con FP-Growth
7. **`output_sync_flow`**: Sincroniza outputs JSON al frontend
8. **`master_flow`**: Ejecuta todos los pipelines en secuencia y sincroniza

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

Ver [DATA_MONITOR_GUIDE.md](DATA_MONITOR_GUIDE.md) para:

- Guía completa del monitor de datos
- Ejemplos de uso avanzado
- Configuración de monitoreo continuo
- Integración con webhooks y APIs
- Troubleshooting del monitor

## 📝 Notas Importantes

- **Tiempo de procesamiento**: Los pipelines pueden tardar varios minutos dependiendo del tamaño de los datos y recursos disponibles
- **Persistencia**: Los datos en PostgreSQL persisten entre reinicios gracias a volúmenes Docker
- **Logs**: Se almacenan en `airflow/logs/` y persisten entre reinicios
- **Recursos**: Se recomienda al menos 8 GB de RAM para ejecutar todos los servicios simultáneamente
- **Resultados**: Los JSON generados están optimizados para consumo desde el frontend React

## 👥 Autores

- Juan David Colonia Aldana – A00395956
- Miguel Ángel Gonzalez Arango – A00395687

## 🧭 Contenido

- [📊 Pipeline de Análisis de Ventas con Apache Spark](#-pipeline-de-análisis-de-ventas-con-apache-spark)
  - [👥 Autores](#-autores)
  - [🧭 Contenido](#-contenido)
  - [🗂️ Descripción de los datos](#️-descripción-de-los-datos)
  - [🔬 Metodología de análisis](#-metodología-de-análisis)
  - [📈 Principales hallazgos visuales](#-principales-hallazgos-visuales)
  - [🧠 Resultados de modelos](#-resultados-de-modelos)
  - [🎯 Conclusiones y aplicaciones empresariales](#-conclusiones-y-aplicaciones-empresariales)
  - [🧵 Pipelines disponibles](#-pipelines-disponibles)
  - [⚙️ Ejecución de pipelines](#️-ejecución-de-pipelines)
  - [Estructura del repositorio (carpeta `airflow/`)](#estructura-del-repositorio-carpeta-airflow)

---

## 🗂️ Descripción de los datos

Los datos originales provienen de transacciones minoristas:

- `transactions/`: archivos `*_Tran.csv` con el histórico de compras. Cada registro trae fecha, tienda, cliente y lista de productos.
- `Categories.csv` y `Product_Categories.csv`: catálogo de productos y su relación con categorías.
- Fuente: entregables del curso.

Se cargan en PostgreSQL mediante los scripts `scripts/linux/windows/load_data.*` y luego Spark accede vía JDBC para todos los pipelines.

---

## 🔬 Metodología de análisis

1. **Ingesta**: Spark lee las tablas principales (`transactions`, `categories`, `product_categories`) directamente desde PostgreSQL.
2. **Enriquecimiento**: se explotan las listas de productos, se calculan métricas por cliente, categoría y fecha, y se estandarizan fechas y tipos numéricos.
3. **Análisis ejecutivo**: agregaciones en Spark SQL para métricas de negocio (transacciones, productos, top-N).
4. **Analítica temporal**: series de tiempo diarias, semanales y mensuales; patrones por día de semana; boxplots por categoría.
5. **Modelos avanzados**:
   - **Clustering**: K-Means con variables de frecuencia, volumen y diversidad de compra.
   - **Recomendaciones**: FP-Growth para reglas de asociación. Se generan salidas por producto y por cliente.
6. **Exportación**: todos los resultados se escriben como JSON en `output/` para ser consumidos por el frontend.

---

## 📈 Principales hallazgos visuales

- **Patrones temporales**: se observan picos los fines de semana y un comportamiento mensual con ligeras estacionalidades.
- **Boxplots por categoría**: algunas familias (por ejemplo, “Frutas y verduras”) concentran la mayor parte de unidades, mientras categorías especializadas tienen variaciones menores.
- **Heatmap**: la correlación revela que frecuencia de compra y diversidad de productos están positivamente relacionadas con el volumen total vendido.

Los archivos JSON en `output/analytics/` contienen las series y distribuciones que alimentan las gráficas del dashboard React.

---

## 🧠 Resultados de modelos

- **Segmentación (K-Means)**  
  Se generan cuatro clusters con perfiles claros:

  1. **VIP/Premium**: alta frecuencia y volumen; candidatos a programas de fidelización robustos.
  2. **Exploradores**: compran gran variedad de productos/categorías; responden bien a lanzamientos.
  3. **Ocasionales**: pocas compras al año; requieren campañas de reactivación.
  4. **Clientes nuevos**: transacciones recientes y de bajo volumen; conviene guiarlos hacia categorías rentables.

- **Recomendaciones (FP-Growth)**
  - **Producto → producto**: reglas con lift > 3 identifican complementos naturales (ej. categorías frescas + abarrotes).
  - **Cliente → producto**: sugerencias personalizadas derivadas de las reglas y del historial individual.
  - Se guardan estadísticas del dataset para evitar recalcular FP-Growth si no cambian los datos.

---

## 🎯 Conclusiones y aplicaciones empresariales

- El pipeline permite monitorear el negocio a nivel ejecutivo, detectar patrones temporales y segmentar clientes sin depender de herramientas externas.
- Las reglas de asociación alimentan estrategias de cross-selling tanto en tienda como en canales digitales.
- Los clusters facilitan campañas específicas: retención de VIPs, incentivos a exploradores, reactivación de clientes ocasionales.
- Exportar en JSON permite integrar fácilmente un dashboard React o cualquier otra aplicación que consuma APIs o archivos estáticos.

---

## 🧵 Pipelines disponibles

| Pipeline            | Objetivo                                                                                                                                  | Salidas principales                                  |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------- |
| `executive_summary` | KPIs ejecutivos: totales de transacciones y ventas, top 10 de productos, clientes y categorías, días pico.                                | `output/summary/basic_metrics.json`, `top_10_*.json` |
| `analytics`         | Series de tiempo (diaria/semanal/mensual), patrones por día de la semana, boxplots por categoría y heatmap de correlaciones.              | `output/analytics/*.json`                            |
| `clustering`        | Segmentación K-Means en cuatro perfiles (VIP, exploradores, ocasionales, nuevos) con métricas y recomendaciones por cluster.              | `output/advanced/clustering/*.json`                  |
| `recommendations`   | Reglas de asociación FP-Growth + sugerencias por producto y por cliente. Reutiliza resultados si las estadísticas del dataset no cambian. | `output/advanced/recommendations/*.json`             |

---

## ⚙️ Ejecución de pipelines

1. **Levantar servicios** (Spark master/worker, PostgreSQL, cliente):

   ```bash
   docker compose up -d
   ```

2. **Cargar datos** (si es la primera vez):

   ```bash
   ./scripts/linux/load_data.sh ../data
   # o en Windows
   scripts/windows/load_data.bat ..\data
   ```

3. **Ejecutar pipelines** dentro del contenedor `spark-client`:

   ```bash
   docker compose exec spark-client python src/run_pipeline.py executive_summary
   docker compose exec spark-client python src/run_pipeline.py analytics
   docker compose exec spark-client python src/run_pipeline.py clustering --n-clusters 4
   docker compose exec spark-client python src/run_pipeline.py recommendations
   ```

4. **Reutilizar FP-Growth**: si los stats de canastas no cambian, el pipeline de recomendaciones reutiliza los resultados previos (`output/data/fp_growth_*`).

5. **Copiar salidas al frontend**: mover `output/summary`, `output/analytics`, `output/advanced/*`, `output/advanced/recommendations` a `sales-frontend/public/data/`.

---

## Estructura del repositorio (carpeta `airflow/`)

```
airflow/
├── docker-compose.yml          # Servicios Spark + PostgreSQL + cliente
├── requirements.txt            # Dependencias (Spark, pandas, sklearn, etc.)
├── src/
│   ├── run_pipeline.py         # CLI para ejecutar pipelines Spark
│   ├── config/spark_config.py  # Configuración del SparkSession
│   ├── pipelines/              # Pipelines: executive, analytics, clustering, recommendations
│   ├── analyzers/              # Lógica de métricas, estadística y modelos
│   ├── data_loader.py          # Lectura JDBC desde PostgreSQL
│   └── json_exporter.py        # Utilidades para escribir JSON
├── scripts/                    # Scripts para cargar datos y ejecutar pipelines (Linux/Windows)
├── data/                       # CSV originales (montar localmente)
└── output/                     # Resultados JSON para el frontend
```

> **Nota**: Esta carpeta se llama `airflow` por el contexto original, pero hoy los pipelines se ejecutan directamente sobre Spark con scripts Dockerizados. No se requiere un scheduler externo.
