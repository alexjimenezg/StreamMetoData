# Meteorisk: Procesamiento y Clasificación de Riesgo Meteorológico en Tiempo Real con Kafka y Spark Structured Streaming

## 1. Descripción

**Meteorisk** es un proyecto que implementa un pipeline
end-to-end de procesamiento de datos meteorológicos en (cuasi) tiempo
real.

El sistema:

- Obtiene datos meteorológicos de **Open-Meteo** para Ciudad de México.
- Publica los eventos en un **tópico Kafka** (`weather_stream`),
  simulando un stream continuo.
- Consume el tópico con **Spark Structured Streaming**, limpia los
  datos y calcula estadísticas por ventanas de tiempo.
- Entrena un modelo supervisado con **Spark MLlib**
  (`RandomForestClassifier`) para clasificar el **nivel de riesgo
  meteorológico** (normal / moderate / critical).
- Aplica el modelo en streaming a los nuevos eventos.
- Visualiza datos, agregados, predicciones, alertas y métricas en un
  **dashboard de Streamlit**.

---

## 2. Objetivo académico

El proyecto demuestra de forma práctica:

- **Ingestión de datos en tiempo real** con Apache Kafka.
- **Procesamiento de streaming** con Spark Structured Streaming.
- **Cálculo de estadísticas por ventana** (windowed aggregations).
- **Almacenamiento en Parquet** como capa intermedia analítica.
- **Entrenamiento de un modelo supervisado** con Spark MLlib.
- **Predicción en streaming** integrando MLlib + Structured Streaming.
- **Visualización interactiva** con Streamlit y Plotly.
- **Comparación de arquitecturas**: ejecución **local** vs
  **Google Colab**, midiendo tiempos y comportamiento del streaming.

---

## 3. Arquitectura general

```
            ┌──────────────────────┐
            │   Open-Meteo API     │
            └──────────┬───────────┘
                       │
                       ▼
                 producer.py
                       │
                       ▼
         Kafka topic: weather_stream
                       │
                       ▼
                 streaming.py
                       │
              ┌────────┴────────┐
              ▼                 ▼
     data/processed/    data/aggregates/
              │
              ▼
              train_model.py
                       │
                       ▼
          models/weather_risk_model
                       │
                       ▼
                 predict_stream.py
                       │
                       ▼
                data/predictions/
                       │
                       ▼
                  dashboard.py
```

---

## 4. Estructura de carpetas

```
Meteorisk/
├── producer.py            # Productor de eventos meteorológicos -> Kafka
├── streaming.py           # Spark Structured Streaming: limpia + agrega + guarda Parquet
├── train_model.py         # Entrenamiento del modelo de riesgo (Spark MLlib)
├── predict_stream.py      # Inferencia en streaming sobre nuevos eventos
├── dashboard.py           # Dashboard Streamlit + Plotly
├── config.py              # Constantes globales (Kafka, rutas, ciudad, modelo)
├── requirements.txt       # Dependencias Python del proyecto
├── docker-compose.yml     # Kafka (bitnami/kafka, modo KRaft) en localhost:9092
├── README.md              # Esta documentación
├── data/
│   ├── raw/               # (opcional) eventos crudos
│   ├── processed/         # Eventos limpios (Parquet) - salida de streaming.py
│   ├── aggregates/        # Estadísticas por ventana (Parquet) - salida de streaming.py
│   ├── predictions/       # Predicciones (Parquet) - salida de predict_stream.py
│   ├── metrics/           # Métricas del modelo (model_metrics.csv)
│   └── checkpoints/       # Checkpoints de Spark Structured Streaming
├── models/
│   └── weather_risk_model/   # Modelo entrenado (formato MLlib)
└── screenshots/           # Capturas para el informe final (Spark UI, dashboard)
```

---

## 5. Requisitos

- **Python 3.10+**
- **Docker Desktop** (para levantar Kafka)
- **Apache Spark / PySpark 3.5.x**
- **Java 11 o 17** (requerido por Spark)
- Librerías Python (incluidas en `requirements.txt`):
  - `kafka-python`
  - `requests`
  - `pyspark`
  - `pandas`
  - `streamlit`
  - `plotly`
  - `pyarrow`

> En Windows, PySpark requiere además `winutils.exe` para operaciones
> de escritura en disco. Consulta la sección de Problemas Comunes.

---

## 6. Instalación

Clona el repositorio y entra en la carpeta del proyecto:

```bash
cd Meteorisk
```

Crea y activa un entorno virtual:

```bash
# Crear el venv
python -m venv venv

# Activar en Windows (PowerShell o CMD)
venv\Scripts\activate

# Activar en Linux / Mac
source venv/bin/activate
```

Instala las dependencias:

```bash
pip install -r requirements.txt
```

---

## 7. Levantar Kafka

Se incluye un `docker-compose.yml` con una instalación mínima de Kafka
(`bitnami/kafka` en modo KRaft, sin Zookeeper).

```bash
docker compose up -d
docker ps
```

Debe aparecer el contenedor **`meteorisk-kafka`** en estado `Up` con el
puerto `9092` mapeado a `localhost`.

Para validar que el tópico `weather_stream` está disponible:

```bash
docker exec -it meteorisk-kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

Para detener Kafka cuando termines:

```bash
docker compose down
```

---

## 8. Ejecución completa del pipeline

Idealmente se utilizan **varias terminales** en paralelo. Cada una se
mantiene corriendo mientras el pipeline está activo.

**Terminal 1 — Kafka + productor**

```bash
docker compose up -d
python producer.py
```

**Terminal 2 — Procesamiento con Spark Structured Streaming**

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 streaming.py
```

**Terminal 3 — Entrenamiento del modelo**

```bash
spark-submit train_model.py
```

**Terminal 4 — Predicción en streaming**

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 predict_stream.py
```

**Terminal 5 — Dashboard**

```bash
streamlit run dashboard.py
```

---

## 9. Orden recomendado de ejecución

1. **`docker compose up -d`** y **`python producer.py`** para empezar a
   publicar eventos en Kafka.
2. **`streaming.py`** para que Spark genere `data/processed/` y
   `data/aggregates/`. Dejar correr unos minutos.
3. **`train_model.py`** para entrenar el modelo y guardar
   `models/weather_risk_model/` + `data/metrics/model_metrics.csv`.
4. **`predict_stream.py`** para generar `data/predictions/`.
5. **`streamlit run dashboard.py`** para visualizar todo.

> Los pasos 2 y 4 pueden ejecutarse simultáneamente: usan distintos
> checkpoints (`data/checkpoints/processed`, `.../aggregates`,
> `.../predictions`) y no compiten entre sí.

---

## 10. Formato del evento JSON

Cada mensaje enviado a Kafka tiene el siguiente formato:

```json
{
  "city": "Mexico City",
  "timestamp": "2026-05-10T14:00",
  "temperature": 27.3,
  "humidity": 65,
  "precipitation": 0.0,
  "wind_speed": 18.2,
  "wind_gusts": 24.1,
  "surface_pressure": 1015.4,
  "apparent_temperature": 28.0,
  "weather_code": 1
}
```

---

## 11. Variables meteorológicas

| Variable               | Unidad   | Descripción                                                                  |
| ---------------------- | -------- | ---------------------------------------------------------------------------- |
| `temperature`          | °C       | Temperatura del aire a 2 m de altura.                                        |
| `humidity`             | %        | Humedad relativa a 2 m.                                                      |
| `precipitation`        | mm       | Precipitación acumulada en la hora.                                          |
| `wind_speed`           | km/h     | Velocidad del viento a 10 m.                                                 |
| `wind_gusts`           | km/h     | Ráfagas máximas de viento a 10 m.                                            |
| `surface_pressure`     | hPa      | Presión atmosférica al nivel de la superficie.                               |
| `apparent_temperature` | °C       | Sensación térmica (combina temperatura, humedad y viento).                   |
| `weather_code`         | código   | Código WMO de condición meteorológica (ver documentación de Open-Meteo).     |
| `city`, `timestamp`    | string   | Metadatos: ciudad y marca de tiempo de la observación.                        |

---

## 12. Clasificación de riesgo

`train_model.py` deriva la variable objetivo **`risk_label`** con reglas
simples (la regla `critical` tiene prioridad sobre `moderate`):

- **`critical` = 2** si se cumple **alguna** de:
  - `temperature > 35`
  - `wind_speed > 60`
  - `precipitation > 50`
- **`moderate` = 1** si se cumple **alguna** de:
  - `30 ≤ temperature ≤ 35`
  - `20 ≤ precipitation ≤ 50`
  - `40 ≤ wind_speed ≤ 60`
- **`normal` = 0** en cualquier otro caso.

Adicionalmente se crea `risk_level` (texto: `"normal"`, `"moderate"`,
`"critical"`) para uso en consola y dashboard.

> `producer.py` incluye un modo `DEMO_MODE=True` que inyecta anomalías
> periódicas (temperatura 36.5, precipitación 55.0, viento 70.0) para
> garantizar que existan casos moderados y críticos en los datos de
> entrenamiento y en el dashboard.

---

## 13. Modelo de Machine Learning

- **Algoritmo:** `RandomForestClassifier` de Spark MLlib.
- **Hiperparámetros:** `numTrees=30`, `maxDepth=5`, `seed=42`.
- **Split:** 80% entrenamiento / 20% prueba (`seed=42`).
- **Métricas evaluadas:** `accuracy`, `weightedPrecision`,
  `weightedRecall`, `f1` con `MulticlassClassificationEvaluator`.

**Features** (orden importante, debe coincidir entre entrenamiento e
inferencia):

1. `temperature`
2. `humidity`
3. `precipitation`
4. `wind_speed`
5. `wind_gusts`
6. `surface_pressure`
7. `apparent_temperature`
8. `weather_code`

---

## 14. Datos generados por el pipeline

| Ruta                                   | Generado por         | Contenido                                                       |
| -------------------------------------- | -------------------- | --------------------------------------------------------------- |
| `data/processed/`                      | `streaming.py`       | Eventos meteorológicos limpios (Parquet).                       |
| `data/aggregates/`                     | `streaming.py`       | Estadísticas por ventana de 1 min, con watermark (Parquet).     |
| `data/predictions/`                    | `predict_stream.py`  | Eventos con predicción de riesgo (Parquet).                     |
| `data/metrics/model_metrics.csv`       | `train_model.py`     | Métricas del modelo (`accuracy`, `f1`, etc.) en CSV.            |
| `data/checkpoints/...`                 | streaming + predict  | Checkpoints de Spark Structured Streaming (no editar a mano).   |
| `models/weather_risk_model/`           | `train_model.py`     | Modelo entrenado en formato MLlib (carpeta).                    |

---

## 15. Dashboard

`dashboard.py` (Streamlit + Plotly) muestra:

- **KPIs meteorológicos**: temperatura promedio, humedad promedio,
  precipitación total, viento máximo, eventos totales.
- **Alertas críticas**: banner rojo si en los datos recientes hay
  `temperature > 35`, `wind_speed > 60`, `precipitation > 50` o el
  modelo predice `critical`.
- **Series temporales**: temperatura, humedad, precipitación, viento
  vs. timestamp.
- **Agregados por ventana**: `avg_temperature`, `total_precipitation`,
  `max_wind_speed` vs. `window_start`.
- **Predicciones**: distribución por `risk_prediction`
  (normal/moderate/critical) y tabla con las últimas 20 predicciones.
- **Métricas del modelo**: accuracy, weightedPrecision, weightedRecall,
  f1, total_rows, clean_rows.
- **Barra lateral**: estado de carga de cada fuente, botón de
  refresco manual y checkbox de auto-refresh cada 10 segundos.

Ejecución:

```bash
streamlit run dashboard.py
```

La app abre en `http://localhost:8501`.

---

## 16. Spark UI

Mientras `streaming.py` o `predict_stream.py` están corriendo, Spark
expone una interfaz web en:

```
http://localhost:4040
```

> Si hay más de una aplicación Spark activa, las siguientes usarán los
> puertos `4041`, `4042`, etc.

**Capturas recomendadas** (guardar en `screenshots/` para el informe):

1. **Jobs** — listado de jobs y duración total.
2. **Stages** — etapas y tiempo de cada una.
3. **SQL / DataFrame** — plan físico de las queries.
4. **Streaming Queries** — input rate, processing rate y batch duration.
5. **Environment** — versión de Spark, Scala, Java y configuración.
6. **Executors** — uso de memoria, GC y tareas por executor.

---

## 17. Métricas para comparar arquitecturas

Tabla rellenada con las mediciones reales del **load test 5 000 ev/s × 60 s**
(300 000 eventos sintéticos) ejecutado el 2026-05-10 en **Local**. La columna
**Google Colab** se completó el 2026-05-11 a partir de los JSON crudos
exportados de la Spark UI en `Colab_Metrics/` (ver § 18).

### 17.1 Hardware

| Recurso          | Local                                        | Google Colab (free runtime)               |
| ---------------- | -------------------------------------------- | ----------------------------------------- |
| CPU              | Intel/AMD x64, 12 cores (driver-only)        | 2 vCPU (driver-only, `local[*]`)          |
| Memoria driver   | 434 MB asignados                              | 434 MB asignados (`maxMemory` = 455 MB)   |
| GPU              | No usada (CPU-only)                          | GPU - T4 |
| Spark version    | 3.5.8                                         | 3.5.0 (pip-installed)                     |
| Java             | OpenJDK 17.0.18                              | OpenJDK 17.0.18 (Ubuntu)                  |
| OS               | Windows 11                                    | Linux (Colab runtime, kernel 6.6.122+)    |
| Almacenamiento   | NVMe local (Parquet en disco)                 | Disco efímero `/content`                  |

### 17.2 Métricas Spark UI — streaming.py (≈60 s, ≈300 000 eventos)

> En Colab la fuente es `rate` (no Kafka): se procesaron **290 000 eventos**
> de salida en **57.22 s** repartidos en 86 microbatches (sinks `processed`
> + `aggregates`), 120 stages, 435 tareas.

| Métrica                                | Local             | Google Colab      | Comentario |
| -------------------------------------- | ----------------: | ----------------: | ---------- |
| Job duration (promedio primeros 10)    | 18.54 s           | 1.45 s            | Colab tarda menos por microbatch porque la fuente `rate` no hace commit Kafka |
| Job duration (rango)                   | 3.18 s – 32.45 s  | 0.11 s – 3.21 s   | Microbatches mucho más cortos en Colab (sin broker) |
| Executor Run Time (acumulado)          | 172.61 s          | 58.63 s           | 2 cores en Colab vs 12 en Local: menos paralelismo → menos tiempo CPU acumulado |
| Scheduler Delay                        | 0 ms              | 0 ms              | Sin contención de recursos en ambos |
| Shuffle Read (top stage)               | 1.10 KB           | 0.74 KB           | Bajo: la ventana agrupa por (city, window) → poco fan-out |
| Shuffle Write (top stage)              | 1.10 KB           | 0.74 KB           | Mismo comportamiento |
| JVM GC Time                            | 1.54 s (acumul.)  | 1.36 s (acumul.)  | < 3 % del runtime — saludable en ambos |
| Spill Memory / Disk                    | 0 (sin spill)     | 0 (sin spill)     | RAM suficiente para el watermark de 2 min |
| Throughput observado (producer)        | **4 388 ev/s**    | n/a (sin Kafka)   | Local: ≥ 4 096 ev/s ✓. Colab no usa broker. |
| Input rate Spark (medio)               | ~5 000 ev/s       | ~5 068 ev/s       | 290 000 ev / 57.22 s ≈ 5 068 ev/s |
| Processing rate Spark                  | ~5 000 ev/s       | ~5 068 ev/s       | Procesamiento al ritmo del input (sin backlog) |
| Batch duration (promedio)              | ~3 – 10 s         | ~0.6 s            | Microbatches mucho más rápidos sin el round-trip al broker |

### 17.3 Métricas Spark UI — predict_stream.py (≈30 s, ≈15 000 eventos)

> Colab: **30 microbatches** sobre **14 500 eventos** procesados en
> **29.00 s** (30 stages, 59 tareas).

| Métrica                          | Local                       | Google Colab                | Comentario |
| -------------------------------- | --------------------------: | --------------------------: | ---------- |
| Total jobs                       | 55                          | 30                          | Menos jobs en Colab: cada microbatch dispara 1 job (sin offset commit extra) |
| Job duration (avg / min / max)   | 0.29 s / 0.04 s / 1.36 s    | 0.157 s / 0.111 s / 0.294 s | Inferencia RandomForest map-only, muy rápida |
| Total shuffle read               | 108 KB                      | 0 B                         | En Colab la inferencia se mantiene map-only (sin reagrupación) |
| Total shuffle write              | 108 KB                      | 0 B                         | Idem |
| Executor run time (acumulado)    | 135.47 s                    | 4.31 s                      | 2 cores; trabajo realmente mínimo por microbatch |
| GC time                          | 0.39 s                      | 0.20 s                      | |
| Predicciones generadas           | 15 000 (normal=13 154, critical=1 771, moderate=75) | 14 500 (desglose por clase no exportado en JSON) | Distribución similar — depende del DEMO_MODE / rate source |

### 17.4 Modelo entrenado (`train_model.py`)

| Métrica              | Local     | Google Colab |
| -------------------- | --------: | -----------: |
| Total filas          | 300 000   | 290 000      |
| accuracy             | 0.9776    | 0.9754       |
| weightedPrecision    | 0.9828    | 0.9762       |
| weightedRecall       | 0.9776    | 0.9754       |
| f1                   | 0.9748    | 0.9757       |
| Tiempo entrenamiento | ~14 s     | 21.45 s      |

> El entrenamiento en Colab es ~1.5× más lento que en Local (21.45 s vs
> ~14 s, con 2 vCPU vs 12 cores) y mueve ~2.7 MB de shuffle entre las
> 39 etapas del `RandomForestClassifier`. La **calidad del modelo es
> equivalente** en ambas arquitecturas: accuracy 0.9776 (Local) vs
> 0.9754 (Colab) y f1 0.9748 vs 0.9757 — diferencias < 0.3 pp,
> atribuibles a la distribución del dataset generado por la fuente
> `rate` frente al productor Kafka.

Los archivos JSON crudos de Spark UI están en `screenshots/local/`
(`spark_jobs.json`, `spark_stages.json`, `spark_executors.json`,
`spark_environment.json`, `predict_*.json`) y en `data/metrics/`
(`load_test_result.csv`, `model_metrics.csv`). Los equivalentes de
**Colab** están en `Colab_Metrics/` (`colab_spark_jobs.json`,
`colab_spark_stages.json`, `colab_spark_executors.json`,
`colab_spark_environment.json`, `model_metrics.csv`).

---

## 18. Comparación local vs Google Colab

El mismo pipeline puede ejecutarse en dos arquitecturas distintas:

- **Local** (este repo): PySpark 3.5.8 sobre Windows 11, 12 cores, RAM
  amplia, Kafka en Docker (`bitnami/kafka` en modo KRaft), almacenamiento
  en disco NVMe local. Productor escrito en Python puro (`kafka-python`),
  ejecutado con el modo `LOAD_TEST_MODE=true` para validar
  ≥ 4 096 eventos/segundo (req. enunciado).
- **Google Colab**: PySpark 3.5.0 sobre runtime de Colab (2 vCPU, ~13 GB
  RAM, sin Docker). Se sustituye Kafka por la fuente `rate` de Spark
  Structured Streaming, que genera filas a velocidad fija y permite
  reproducir las mismas transformaciones (limpieza, ventana con
  watermark, entrenamiento RandomForest, inferencia en streaming).
  Reproducible en `Meteorisk_Colab.ipynb`.

### 18.1 Cómo ejecutar la comparativa Colab

1. Subir `Meteorisk_Colab.ipynb` a colab.research.google.com.
2. Ejecutar las celdas en orden (≈ 4 min totales).
3. La última celda descarga `meteorisk_colab_metrics.zip` con los
   JSON de Spark UI; volcar los valores en las columnas vacías de §17.

### 18.2 Conclusiones (preliminar — completar tras correr Colab)

- **Local + Kafka real** es la configuración más fiel al caso de
  producción: hay broker, hay offsets, hay confirmaciones de commit,
  hay back-pressure real. Es la única que **prueba el throughput
  Kafka end-to-end** (4 388 ev/s observados).
- **Colab + rate source** es válida para benchmarking de la
  **lógica Spark** (ventanas, watermark, MLlib) pero no mide el
  broker. Su utilidad principal es comparar **costes de procesamiento
  Spark** entre dos hardware muy distintos cuando se quita el broker
  como variable.
- Para un piloto real se recomienda local-Docker durante el
  desarrollo (iteración rápida, UI accesible en `localhost:4040`) y
  EMR / Dataproc para producción (autoscaling, persistencia S3/GCS).
- Colab gratis tiene timeouts (~12 h máx, kicks por inactividad ~1.5 h)
  y `/content` es efímero; no es adecuado para streaming 24/7 ni
  para conservar checkpoints entre ejecuciones.

---

## 19. Problemas comunes y soluciones

| Problema                                                             | Causa probable                                                                       | Solución                                                                                                                            |
| -------------------------------------------------------------------- | ------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- |
| **Kafka no levanta**                                                 | Docker no está corriendo o el puerto 9092 está ocupado.                              | Iniciar Docker Desktop, liberar el puerto 9092 o cambiarlo en `docker-compose.yml`. Volver a hacer `docker compose up -d`.          |
| **`NoBrokersAvailable`** en `producer.py` / `streaming.py`           | Kafka aún no terminó de arrancar o el contenedor está detenido.                      | Esperar 10–20 s tras `docker compose up -d`; verificar con `docker ps`. Confirmar `localhost:9092`.                                 |
| **`ClassNotFoundException: ...KafkaSourceProvider`**                 | Falta el paquete `spark-sql-kafka` al ejecutar streaming/predict.                    | Usar `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 ...`. Ajustar la versión si tu PySpark no es 3.5.x.  |
| **`data/processed/` vacío**                                          | `streaming.py` no llegó a hacer commit, o `producer.py` no está publicando.          | Dejar correr el productor y el streaming varios minutos. Verificar archivos `part-*.parquet` en la carpeta.                         |
| **`data/aggregates/` vacío durante mucho tiempo**                    | Comportamiento esperado: watermark de 2 min + ventanas de 1 min en modo `append`.    | Esperar 5–10 minutos. La consola del streaming sí muestra los agregados de inmediato (modo `complete`).                             |
| **`No se encontró el modelo en models/weather_risk_model`**          | `train_model.py` no se ejecutó, o se ejecutó en otra carpeta.                        | Ejecutar `spark-submit train_model.py` desde `Meteorisk/`. Comprobar que existe `models/weather_risk_model/` con `data/` y `metadata/`. |
| **Dashboard sin datos** (todo en rojo en la sidebar)                 | Aún no se han generado los Parquet / CSV de origen.                                  | Ejecutar el pipeline en el orden recomendado y refrescar el dashboard con el botón "Actualizar datos".                              |
| **`HADOOP_HOME and hadoop.home.dir are unset` (Windows)**            | PySpark en Windows necesita `winutils.exe` para escritura a disco.                   | Descargar `winutils.exe` compatible con tu versión de Hadoop, ubicarlo en `C:\hadoop\bin\` y exportar `HADOOP_HOME=C:\hadoop`.       |
| **`ImportError: Missing optional dependency 'pyarrow'`** (dashboard) | Falta `pyarrow` en el entorno donde corre Streamlit.                                 | Activar el venv del proyecto antes de `streamlit run ...` y reinstalar `requirements.txt`.                                          |

---

## 20. Checklist final de entrega

- [ ] `producer.py` envía eventos JSON al tópico `weather_stream`.
- [ ] `streaming.py` genera Parquet en `data/processed/` y
      `data/aggregates/`.
- [ ] `train_model.py` guarda el modelo en
      `models/weather_risk_model/` y las métricas en
      `data/metrics/model_metrics.csv`.
- [ ] `predict_stream.py` genera Parquet en `data/predictions/` con la
      columna `risk_prediction`.
- [ ] `dashboard.py` visualiza KPIs, gráficas, predicciones, alertas y
      métricas del modelo.
- [ ] Capturas de **Spark UI** guardadas en `screenshots/`
      (Jobs, Stages, SQL/DataFrame, Streaming, Environment, Executors).
- [ ] Tabla **Local vs Google Colab** completada con datos reales.
- [ ] Informe final redactado en español.
