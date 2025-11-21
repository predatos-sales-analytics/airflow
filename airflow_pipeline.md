# Mapeo del pipeline actual hacia Airflow

Este documento resume los componentes existentes del proyecto y cómo se traducirán a DAGs/tareas en Apache Airflow.

## Fuentes y utilidades clave

- `config/spark_config.py`: crea/termina sesiones Spark (actualmente en `local[1]`, se adaptará a modo distribuido para FP-Growth).
- `src/data_loader.py`: carga categorías, productos-categorías y transacciones (permite filtrar por tienda) y provee `explode_transactions`.
- `src/analyzers/*.py`:
  - `product_analyzer.py`: top productos, productos por tienda, preparación de canastas y FP-Growth.
  - `customer_analyzer.py`: resumenes RFM/frecuencia de clientes.
  - `temporal_analyzer.py`: resúmenes temporales (semanal, mensual, día, tendencias).
- `src/visualizer.py`: genera artefactos en `output/`.

## Fases actuales del pipeline

Basado en `src/pipeline.py`, el flujo principal abarca:

1. **Inicialización (`SalesAnalyticsPipeline.initialize`)**

   - Arranca Spark y crea instancias de `DataLoader`, analyzers y visualizador.

2. **Análisis de tablas de referencia**

   - `run_categories_analysis`
   - `run_product_categories_analysis`

3. **Análisis de transacciones base (`run_transactions_analysis`)**

   - Carga completa (o muestra) de transacciones por tienda.
   - Métricas de calidad, cardinalidades y visualizaciones iniciales.

4. **Análisis con transacciones explodidas (`run_transactions_exploded_analysis`)**

   - Genera dataset detallado por producto.

5. **Análisis avanzados (`run_advanced_analysis`)**

   - Reutiliza transacciones cacheadas y ejecuta, en orden:
     - `run_temporal_analysis`
     - `run_customer_analysis`
     - `run_product_analysis` (incluye Market Basket con FP-Growth y generación de reglas).

6. **Finalización (`finalize`)**
   - Reportes finales, resumen de gráficos y cierre de Spark.

## Traducción propuesta a Airflow

| Pipeline actual                        | DAG/Tarea Airflow propuesta               | Notas                                                                                                                    |
| -------------------------------------- | ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| Inicialización global                  | `bootstrap_environment` (TaskFlow)        | Configura variables/conexiones y parámetros Spark.                                                                       |
| Análisis categorías                    | `analyze_categories`                      | Escalable como tarea independiente.                                                                                      |
| Análisis producto-categoría            | `analyze_product_categories`              | Depende de análisis categorías.                                                                                          |
| Análisis transacciones                 | `analyze_transactions`                    | Lee todas las tiendas o subset; produce métricas base.                                                                   |
| Explosión de transacciones             | `explode_transactions`                    | Produce tabla intermedia (puede escribirse a Postgres/S3).                                                               |
| Análisis temporal                      | `temporal_analysis`                       | Usa dataset de transacciones completo.                                                                                   |
| Análisis clientes                      | `customer_analysis`                       | Usa dataset transacciones.                                                                                               |
| Análisis productos                     | `product_analysis_master`                 | Coordina subtareas por tienda y FP-Growth.                                                                               |
| Subanálisis por tienda                 | `@task.dynamic` `analyze_store(store_id)` | Ejecuta lógica de `ProductAnalyzer` filtrada por tienda en paralelo.                                                     |
| FP-Growth distribuido                  | `train_fp_growth_distributed`             | Trabaja sobre todas las canastas, ejecutándose en cluster Spark (modo `standalone`/`k8s` configurado en `spark_config`). |
| Generación de visualizaciones/reportes | `generate_reports`                        | Puede consolidar resultados almacenados.                                                                                 |
| Finalización                           | `cleanup`                                 | Limpia temporales, archiva logs y detiene recursos.                                                                      |

## Entradas/Salidas entre tareas

- Datos crudos `.csv` permanecen en `data/`. Las tareas Airflow deben leer desde volúmenes compartidos o almacenamientos externos.
- Resultados intermedios recomendados:
  - Tablas en Postgres (metadatos, resumenes por tienda).
  - Archivos Parquet/CSV en volúmenes `airflow/data` para consumo por tareas siguientes.
- Artefactos finales siguen en `output/` (montado como volumen para el contenedor `airflow-webserver` para consulta).

## Ejecución paralela por tienda

- Utilizar `DataLoader.get_available_stores()` dentro de una tarea para obtener IDs.
- Aplicar `task.expand(store_id=store_ids)` (Dynamic Task Mapping) para procesar cada tienda en paralelo, alimentando análisis específicos y permitiendo escalamiento horizontal.

## FP-Growth distribuido

- Configurar Spark para modo cluster mediante variables (`SPARK_MASTER_URL`, `SPARK_SUBMIT_ARGS`) y garantizar que la tarea `train_fp_growth_distributed` utilice `spark-submit` o `SparkSubmitOperator`.
- El modelo seguirá exportando itemsets/reglas a `output/data` y a la base Postgres para consumo downstream.

## Consideraciones adicionales

- Al mover lógica a DAGs, mantener reutilización del código existente importando módulos desde `src` (evitar duplicar lógica).
- Centralizar configuraciones (paths, parámetros de soporte/confianza) vía Variables Airflow o un archivo `.env`.
- Asegurar que los DAGs validen la disponibilidad de datos antes de ejecutar (sensors o chequeo directo en Postgres/volúmenes).
