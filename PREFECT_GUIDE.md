# 📘 Guía Completa de Prefect para el Proyecto

Esta guía proporciona información detallada sobre la implementación de Prefect en el proyecto de análisis de ventas.

## 📋 Tabla de Contenidos

- [¿Qué es Prefect?](#qué-es-prefect)
- [Arquitectura del Proyecto](#arquitectura-del-proyecto)
- [Flows Disponibles](#flows-disponibles)
- [Configuración](#configuración)
- [Uso Básico](#uso-básico)
- [Uso Avanzado](#uso-avanzado)
- [Monitoreo y Debugging](#monitoreo-y-debugging)
- [Troubleshooting](#troubleshooting)
- [Mejores Prácticas](#mejores-prácticas)

## ¿Qué es Prefect?

**Prefect** es un framework moderno de orquestación de flujos de trabajo que permite:

- **Observabilidad total**: UI web para visualizar ejecuciones en tiempo real
- **Recuperación automática**: Retry y manejo de errores configurable
- **Flexibilidad**: Ejecutar flows manualmente, programados, o mediante API
- **Desarrollo ágil**: Flows en Python puro, fácil de testear y mantener
- **Escalabilidad**: Desde desarrollo local hasta producción distribuida

### ¿Por qué migramos de scripts manuales a Prefect?

**Antes (Scripts manuales)**:
- Ejecutar comandos bash/bat manualmente
- Sin visibilidad del progreso
- Difícil rastrear errores
- Sin recuperación automática de fallos
- Logs dispersos en diferentes lugares

**Ahora (Prefect)**:
- Ejecución automatizada con un solo comando
- UI web con progreso en tiempo real
- Logs centralizados por task
- Retry automático en fallos transitorios
- Sincronización automática de resultados al frontend

## Arquitectura del Proyecto

### Componentes

```
┌─────────────────────────────────────────────────────────────┐
│                    Usuario / Developer                       │
└───────────────┬─────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────┐
│              Prefect UI (http://localhost:4200)             │
│                    (Dashboard & Monitoring)                  │
└───────────────┬─────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────┐
│                     Prefect Server                           │
│              (Gestión de flows y estados)                    │
└───────────────┬─────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────┐
│                     Prefect Worker                           │
│               (Ejecuta flows y tasks)                        │
└─────┬─────────────────┬───────────────────┬─────────────────┘
      │                 │                   │
      ▼                 ▼                   ▼
┌──────────┐    ┌──────────────┐    ┌──────────────┐
│PostgreSQL│    │ Spark Cluster│    │   Frontend   │
│  (Datos) │    │(Procesamiento)│    │ (Resultados) │
└──────────┘    └──────────────┘    └──────────────┘
```

### Servicios Docker

1. **prefect-server**: API y base de datos de Prefect
   - Puerto: 4200
   - UI web para monitoreo
   - Almacena metadatos de flows y ejecuciones

2. **prefect-worker**: Ejecutor de flows
   - Conectado al Prefect Server
   - Ejecuta tasks con acceso a Spark, PostgreSQL y filesystem
   - Procesa flows de la cola `default-pool`

3. **postgres**: Base de datos de ventas
4. **spark-master** y **spark-worker**: Cluster Spark para procesamiento
5. **spark-client**: Cliente Spark (legacy, para ejecución directa)

## Flows Disponibles

### 1. `data_loading_flow`

**Propósito**: Cargar datos CSV a PostgreSQL de forma automatizada.

**Tasks**:
1. Verificar que existan archivos CSV
2. Crear tablas en PostgreSQL
3. Limpiar tablas existentes (opcional)
4. Cargar categorías
5. Cargar productos-categorías
6. Cargar transacciones

**Ejecución**:
```bash
# Linux/Mac
./scripts/linux/run_prefect_flow.sh data_loading

# Windows
scripts\windows\run_prefect_flow.bat data_loading
```

**Parámetros**:
- `truncate` (bool): Si True, limpia las tablas antes de cargar (default: True)

### 2. `executive_summary_flow`

**Propósito**: Generar resumen ejecutivo con métricas clave.

**Outputs**:
- `output/summary/basic_metrics.json`
- `output/summary/top_10_products.json`
- `output/summary/top_10_customers.json`
- `output/summary/top_10_categories.json`

**Ejecución**:
```bash
./scripts/linux/run_prefect_flow.sh executive_summary
```

**Duración típica**: 3-5 minutos

### 3. `analytics_flow`

**Propósito**: Análisis temporal y correlaciones.

**Outputs**:
- `output/analytics/daily_sales.json`
- `output/analytics/variable_correlation.json`
- `output/analytics/weekday_pattern.json`

**Ejecución**:
```bash
./scripts/linux/run_prefect_flow.sh analytics
```

**Duración típica**: 5-8 minutos

### 4. `clustering_flow`

**Propósito**: Segmentación de clientes usando K-Means.

**Outputs**:
- `output/advanced/clustering/cluster_summary.json`
- `output/advanced/clustering/clustering_visualization.json`

**Ejecución**:
```bash
./scripts/linux/run_prefect_flow.sh clustering
```

**Parámetros**:
- `n_clusters` (int): Número de clusters (default: 4)

**Duración típica**: 8-12 minutos

### 5. `recommendations_flow`

**Propósito**: Sistema de recomendaciones con FP-Growth.

**Outputs**:
- `output/recommendations/product_recs.json`
- `output/recommendations/customer_recs.json`

**Ejecución**:
```bash
./scripts/linux/run_prefect_flow.sh recommendations
```

**Parámetros**:
- `min_support` (float): Soporte mínimo (default: 0.005)
- `min_confidence` (float): Confianza mínima (default: 0.2)

**Duración típica**: 10-15 minutos

### 6. `output_sync_flow`

**Propósito**: Sincronizar outputs JSON al frontend.

**Acciones**:
1. Validar que existan archivos JSON en `output/`
2. Copiar a `../sales-frontend/public/data/`
3. Mantener estructura de directorios

**Ejecución**:
```bash
./scripts/linux/run_prefect_flow.sh output_sync
```

**Duración típica**: < 1 minuto

### 7. `master_flow` ⭐

**Propósito**: Ejecutar todos los pipelines en secuencia y sincronizar.

**Flujo de ejecución**:
1. ✓ Validar datos en PostgreSQL
2. → Ejecutar `executive_summary_flow`
3. → Ejecutar `analytics_flow`
4. → Ejecutar `clustering_flow`
5. → Ejecutar `recommendations_flow`
6. → Ejecutar `output_sync_flow`

**Ejecución**:
```bash
./scripts/linux/run_prefect_flow.sh master
```

**Parámetros**:
- `n_clusters` (int): Para clustering (default: 4)
- `min_support` (float): Para recomendaciones (default: 0.005)
- `min_confidence` (float): Para recomendaciones (default: 0.2)
- `sync_to_frontend` (bool): Si sincroniza al frontend (default: True)

**Duración típica**: 30-45 minutos

**Ventajas**:
- Ejecución completa con un solo comando
- Continúa aunque falle un pipeline individual
- Reporte final con resumen de éxitos/fallos

## Configuración

### Variables de Entorno

Las variables están configuradas en `docker-compose.yml` para el servicio `prefect-worker`:

```yaml
environment:
  - PREFECT_API_URL=http://prefect-server:4200/api
  - POSTGRES_HOST=postgres
  - POSTGRES_PORT=5432
  - POSTGRES_DB=sales
  - POSTGRES_USER=sales
  - POSTGRES_PASSWORD=sales
  - SPARK_MASTER_URL=spark://spark-master:7077
  - DATA_PATH=/opt/prefect/work-dir/data
  - OUTPUT_PATH=/opt/prefect/work-dir/output
  - FRONTEND_PATH=/opt/prefect/work-dir/frontend
```

### Configuración de Prefect

El archivo `src/prefect_config.py` centraliza la configuración:

```python
from prefect_config import (
    get_prefect_api_url,
    get_postgres_config,
    get_spark_config,
    get_paths_config,
)
```

## Uso Básico

### Inicializar Prefect (primera vez)

Después de levantar los servicios con `docker compose up -d`:

```bash
# Linux/Mac
./scripts/linux/setup_prefect.sh

# Windows
scripts\windows\setup_prefect.bat
```

Este script:
1. Espera a que Prefect Server esté listo
2. Crea el work pool `default-pool`
3. Muestra información de acceso

### Ejecutar un Flow

```bash
# Linux/Mac
./scripts/linux/run_prefect_flow.sh <flow_name>

# Windows
scripts\windows\run_prefect_flow.bat <flow_name>
```

### Acceder a la UI

Abre en tu navegador: **http://localhost:4200**

La UI muestra:
- **Flow Runs**: Historial de ejecuciones
- **Flows**: Flows registrados
- **Work Queues**: Colas de trabajo
- **Logs**: Logs en tiempo real

## Uso Avanzado

### Ejecutar Flow desde Python

Puedes ejecutar flows directamente desde Python:

```bash
docker compose exec prefect-worker python -m flows.master_flow
```

O entrar al contenedor:

```bash
docker compose exec -it prefect-worker bash
cd /opt/prefect/work-dir/src
python -m flows.master_flow
```

### Modificar Parámetros de Flows

Para personalizar parámetros, edita los archivos de flows en `src/flows/` y modifica los valores por defecto en las funciones de flow.

Por ejemplo, en `clustering_flow.py`:

```python
@flow(...)
def clustering_flow(n_clusters: int = 5):  # Cambiar de 4 a 5
    ...
```

### Crear Nuevos Flows

1. Crea un nuevo archivo en `src/flows/`, por ejemplo `my_custom_flow.py`
2. Importa las dependencias necesarias:

```python
from prefect import flow, task
from flows.notifications import get_notification_service

@task(name="Mi Task Personalizada", retries=2)
def my_custom_task():
    notifier = get_notification_service()
    notifier.log_task_start("Mi Task Personalizada")
    
    try:
        # Tu lógica aquí
        result = "¡Éxito!"
        
        notifier.log_task_success("Mi Task Personalizada", result)
        return result
    except Exception as e:
        notifier.log_task_failure("Mi Task Personalizada", e)
        raise

@flow(name="Mi Flow Personalizado", log_prints=True)
def my_custom_flow():
    notifier = get_notification_service()
    notifier.log_flow_start("my_custom_flow")
    
    try:
        result = my_custom_task()
        notifier.log_flow_success("my_custom_flow")
        return result
    except Exception as e:
        notifier.log_flow_failure("my_custom_flow", e)
        raise
```

3. Agregar al script de ejecución en `scripts/*/run_prefect_flow.*`

### Sistema de Notificaciones

Todos los flows usan el sistema de notificaciones en `src/flows/notifications.py`:

```python
from flows.notifications import get_notification_service

notifier = get_notification_service()

# Diferentes tipos de notificaciones
notifier.log_info("Información general")
notifier.log_success("Operación exitosa")
notifier.log_warning("Advertencia")
notifier.log_error("Error ocurrido")

# Notificaciones de flow
notifier.log_flow_start("flow_name", {"param": "value"})
notifier.log_flow_success("flow_name", duration_seconds=120.5)
notifier.log_flow_failure("flow_name", exception)

# Notificaciones de task
notifier.log_task_start("task_name")
notifier.log_task_success("task_name", "resultado opcional")
notifier.log_task_failure("task_name", exception)
```

Las notificaciones se registran en:
- Consola (con colores si colorama está disponible)
- Archivo JSON: `logs/prefect_runs/prefect_runs_YYYY-MM-DD.jsonl`

## Monitoreo y Debugging

### UI de Prefect

Accede a http://localhost:4200 para:

1. **Dashboard**: Vista general de ejecuciones recientes
2. **Flow Runs**: Lista de todas las ejecuciones
   - Estado: Running, Completed, Failed, Cancelled
   - Duración
   - Parámetros utilizados
3. **Logs**: Logs detallados por task y flow
4. **Gráfico de Flow**: Visualización de dependencias entre tasks

### Logs en Tiempo Real

Ver logs del worker:

```bash
docker compose logs -f prefect-worker
```

Ver logs de todos los servicios:

```bash
docker compose logs -f
```

### Logs Persistentes

Los logs de Prefect se guardan automáticamente en:

```
logs/prefect_runs/prefect_runs_YYYY-MM-DD.jsonl
```

Cada línea es un JSON con:
- `timestamp`: Momento del evento
- `event`: Tipo de evento (flow_start, flow_success, flow_failure, etc.)
- `flow_name` o `task_name`: Nombre del componente
- Datos adicionales según el evento

### Debugging de Flows

Si un flow falla:

1. **Revisar la UI**: http://localhost:4200 → Flow Runs → Click en la ejecución fallida
2. **Ver logs específicos**: Click en el task que falló
3. **Stack trace**: Disponible en la UI
4. **Re-ejecutar**: Botón "Re-run" en la UI para reintentar

Para debugging más profundo:

```bash
# Entrar al contenedor
docker compose exec -it prefect-worker bash

# Activar Python debugger
cd /opt/prefect/work-dir/src
python -m pdb -m flows.my_flow
```

## Troubleshooting

### Problema: Prefect Server no responde

**Síntomas**: No se puede acceder a http://localhost:4200

**Solución**:
```bash
# Verificar estado del contenedor
docker compose ps prefect-server

# Ver logs
docker compose logs prefect-server

# Reiniciar
docker compose restart prefect-server

# Esperar a que esté listo
docker compose exec prefect-server curl http://localhost:4200/api/health
```

### Problema: Worker no ejecuta flows

**Síntomas**: Flows quedan en estado "Pending"

**Solución**:
```bash
# Verificar worker
docker compose ps prefect-worker
docker compose logs prefect-worker

# Reiniciar worker
docker compose restart prefect-worker

# Verificar work pool
docker compose exec prefect-worker prefect work-pool ls
```

### Problema: Flow falla por conexión a PostgreSQL

**Síntomas**: Error "could not connect to server"

**Solución**:
```bash
# Verificar PostgreSQL
docker compose ps postgres

# Test de conexión
docker compose exec postgres psql -U sales -d sales -c "SELECT COUNT(*) FROM transactions;"

# Verificar red
docker compose exec prefect-worker ping postgres
```

### Problema: Flow falla por conexión a Spark

**Síntomas**: Error "Unable to connect to Spark Master"

**Solución**:
```bash
# Verificar Spark Master
docker compose ps spark-master

# Ver UI de Spark
# http://localhost:8082

# Reiniciar Spark
docker compose restart spark-master spark-worker
```

### Problema: Archivos no se sincronizan al frontend

**Síntomas**: `output_sync_flow` no copia archivos

**Solución**:
```bash
# Verificar que existen outputs
ls -la output/summary/

# Verificar permisos
docker compose exec prefect-worker ls -la /opt/prefect/work-dir/output/

# Verificar montaje del volumen
docker compose exec prefect-worker ls -la /opt/prefect/work-dir/frontend/

# Ejecutar sync manualmente
docker compose exec prefect-worker python -m flows.output_sync_flow
```

### Problema: "No module named 'flows'"

**Síntomas**: ImportError al ejecutar flows

**Solución**:
```bash
# Verificar PYTHONPATH
docker compose exec prefect-worker echo $PYTHONPATH

# Debe ser: /opt/prefect/work-dir/src

# Verificar archivos
docker compose exec prefect-worker ls /opt/prefect/work-dir/src/flows/

# Si falta __init__.py
docker compose exec prefect-worker touch /opt/prefect/work-dir/src/flows/__init__.py
```

## Mejores Prácticas

### 1. Desarrollo Iterativo

- Testea flows localmente primero (desde Python directamente)
- Usa print() o logging para debugging
- Una vez funcional, ejecuta desde Prefect

### 2. Manejo de Errores

- Usa `retries` en tasks que pueden fallar transitoriamente
- Ej: `@task(retries=3, retry_delay_seconds=10)`
- Captura y loggea excepciones específicas

### 3. Idempotencia

- Diseña flows que puedan re-ejecutarse sin efectos secundarios
- Usa `truncate=True` en `data_loading_flow` para limpiar antes de cargar
- Verifica existencia antes de crear recursos

### 4. Logging Efectivo

- Usa el servicio de notificaciones consistentemente
- Loggea inicio, éxito y fallo de cada operación importante
- Incluye contexto útil (ej: número de registros procesados)

### 5. Monitoreo

- Revisa la UI regularmente después de ejecuciones
- Configura alertas si planeas automatizar (Prefect Cloud)
- Mantén logs históricos para análisis

### 6. Parámetros

- Define parámetros con valores por defecto razonables
- Documenta qué hace cada parámetro
- Valida parámetros al inicio del flow

### 7. Estructura de Código

- Separa lógica de negocio (pipelines) de orquestación (flows)
- Reutiliza componentes existentes
- Mantén flows simples y componibles

### 8. Versionado

- Los flows están en código, versionalos con Git
- Documenta cambios significativos en flows
- Considera tags o branches para diferentes versiones

## Recursos Adicionales

### Documentación Oficial

- [Prefect Docs](https://docs.prefect.io/)
- [Prefect Concepts](https://docs.prefect.io/concepts/)
- [Prefect Recipes](https://docs.prefect.io/recipes/)

### Comunidad

- [Prefect Slack](https://prefect.io/slack)
- [Prefect Discourse](https://discourse.prefect.io/)
- [GitHub Issues](https://github.com/PrefectHQ/prefect)

### En este Proyecto

- [README.md](README.md): Documentación general del proyecto
- `src/flows/`: Código fuente de los flows
- `src/prefect_config.py`: Configuración centralizada
- `docker-compose.yml`: Definición de servicios

---

## Autores

- Juan David Colonia Aldana - A00395956
- Miguel Ángel Gonzalez Arango - A00395687

**Universidad del Valle - 2025**

