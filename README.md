# 📊 Pipeline de Análisis de Ventas con Apache Spark

Pipelines de procesamiento desarrollados en Apache Spark para analizar el comportamiento de ventas, clientes y productos. El proyecto se ejecuta dentro del directorio `airflow/`, pero toda la orquestación se realiza con scripts ligeros y contenedores Spark + PostgreSQL.

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
