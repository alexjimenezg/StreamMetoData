# Meteorisk Runbook

This file is the quick run guide for the rubric. It tells you what already works, what is still missing, and the exact commands to run on Windows PowerShell.

Important: in PowerShell use `;` between commands, not `&&`.

---

## Demo final del examen — comandos exactos (copy / paste)

Esta es la secuencia mínima para presentar al profesor: captura del flujo desde Open-Meteo, predicción del modelo en tiempo real y visualización en un dashboard live. Toma capturas de pantalla del navegador en cada paso.

### Setup (una sola vez, si no lo has hecho ya)

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
python -m venv venv
venv\Scripts\python.exe -m pip install -r requirements.txt
```

Verifica que el modelo ya esté entrenado:

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
dir models\weather_risk_model
```

Si la carpeta `models\weather_risk_model` está vacía, entrena el modelo primero (ver sección "Opcional · Entrenamiento" más abajo).

### Demo mínima (3 terminales) — lo indispensable para el profesor

Abre **3 terminales PowerShell**. En todas ejecuta primero `cd C:\StreamMetoData\StreamMetoData\Meteorisk`.

**Terminal 1 — Kafka (broker de mensajes)**

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
docker compose up -d
docker compose ps
```

Espera a ver el contenedor `Up`. No cierres esta terminal.

**Terminal 2 — Productor (captura del flujo Open-Meteo → Kafka)**

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
venv\Scripts\python.exe producer.py
```

Verás logs como `Evento enviado -> {...}`. Déjalo corriendo. El modo `DEMO_MODE=True` inyecta anomalías cada 15/25/40 eventos para que se vean las tres clases (`normal`, `moderate`, `critical`) en el dashboard.

**Terminal 3 — Dashboard en vivo (consume Kafka + clasifica en tiempo real)**

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
venv\Scripts\python.exe -m streamlit run live_dashboard.py
```

Abre el navegador en `http://localhost:8501`. Deberías ver:

- Banner `🔴 LIVE` con "Última actualización: hace X s" (donde X < 5).
- KPIs del último evento (temp, humedad, precip, viento) y un badge con la predicción de color.
- Contadores acumulados `🟢 normal / 🟠 moderate / 🔴 critical`.
- Mini-gráficas de temperatura y predicción a lo largo del tiempo.
- Tabla con los últimos 15 eventos coloreada por nivel de riesgo.

**Capturas recomendadas:** una con condición `normal`, una con `moderate` y una con `critical` (espera ~30 s para que entren las anomalías).

### Demo completa (5 terminales) — incluye pipeline Spark + dashboard histórico

Repite los pasos 1, 2 y 3 anteriores y añade:

**Terminal 4 — Spark Structured Streaming (Kafka → Parquet procesado + agregados)**

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=data\spark_events C:\StreamMetoData\StreamMetoData\Meteorisk\streaming.py
```

**Terminal 5a — Spark predict stream (Kafka → modelo MLlib → Parquet predicciones)**

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=data\spark_events C:\StreamMetoData\StreamMetoData\Meteorisk\predict_stream.py
```

**Terminal 5b — Dashboard histórico de Streamlit (puerto distinto: 8502)**

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
venv\Scripts\python.exe -m streamlit run dashboard.py --server.port 8502
```

Abre `http://localhost:8502` (vista histórica con series temporales, agregados por ventana de 1 min y métricas del modelo) en paralelo con `http://localhost:8501` (vista live).

### Opcional · Spark UI para capturas del rubric

Para tener el History Server activo y poder capturar Jobs / Stages / Streaming:

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
md data\spark_events -ErrorAction SilentlyContinue
spark-class org.apache.spark.deploy.history.HistoryServer --logDirectory "data\spark_events"
```

Abre `http://localhost:18080` (persistente) o `http://localhost:4040` (mientras un job corre).

### Opcional · Entrenamiento (solo si `models\weather_risk_model` está vacío)

Necesitas que hayan corrido `producer.py` + `streaming.py` un par de minutos para tener datos en `data\processed\`. Luego:

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
spark-submit --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=data\spark_events C:\StreamMetoData\StreamMetoData\Meteorisk\train_model.py
```

Las métricas quedan en `data\metrics\model_metrics.csv` y el modelo en `models\weather_risk_model\`.

### Opcional · Modo de carga para Spark UI metrics (≥4 k ev/s)

En la terminal del productor, antes de arrancarlo:

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
$env:LOAD_TEST_MODE = "true"
$env:LOAD_TEST_RATE = "5000"
$env:LOAD_TEST_DURATION = "300"
$env:LOAD_TEST_ANOMALY_PROB = "0.05"
venv\Scripts\python.exe producer.py
```

El resultado del load test se guarda en `data\metrics\load_test_result.csv`.

### Cierre limpio (al terminar la demo)

1. `Ctrl+C` en cada terminal de Streamlit, Spark y producer (en ese orden).
2. Apagar Kafka:

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
docker compose down
```

### Capturas a entregar (sugerencia)

Guarda los `.png` en `docs\screenshots\` (créala si no existe):

- `01_live_normal.png` — `live_dashboard.py` mostrando condición normal (verde).
- `02_live_moderate.png` — banner naranja por anomalía moderada.
- `03_live_critical.png` — banner rojo con riesgo crítico.
- `04_dashboard_historico.png` — `dashboard.py` con series temporales y métricas del modelo.
- `05_spark_ui_streaming.png` — pestaña "Streaming" del Spark UI con input/processing rate.

---

## What is already working

- Data origin is simulated / near real time: `producer.py` reads Open-Meteo and publishes weather events to Kafka, and it also supports synthetic load mode for stress testing.
- Local architecture is working end to end: Kafka -> Spark Structured Streaming -> Parquet -> Spark ML model -> prediction stream.
- Unit tests and integration tests are passing.
- Model training works and saves a timestamped model under `models/`.
- Dashboard runs in Streamlit and reads processed data, aggregates, predictions, and model metrics.

## What is still missing for the rubric

- A documented comparison between two architectures with evidence from Spark UI.
- At least two measured Spark UI metrics captured for each architecture.
- A BI tool other than Streamlit if your professor strictly requires Qlik, Power BI, or Tableau.
- Screenshots or exported evidence for the report.

## Rubric checklist

### 0. Origin and type of data flow

- Status: working, but simulated / near real time rather than a physical real-time sensor source.
- Evidence: Open-Meteo -> Kafka -> Spark.

### 1. Two architectures + Spark UI comparison

- Status: partially done.
- Working architecture in this repo: local Windows + Docker Kafka + Spark Structured Streaming + Spark MLlib + Streamlit.
- Second architecture for comparison: the repo mentions a Colab-based option, but the benchmark comparison still needs to be captured and documented.
- Metrics to capture in Spark UI: execution time, shuffle read/write, scheduler delay, spill, input rate, processing rate, and batch duration.

### 2. Real-time traffic indicators

- Status: partially done.
- Working: Streamlit dashboard shows metrics, charts, alerts, and predictions.
- Missing if the rubric is strict: a BI tool such as Power BI, Tableau, or Qlik.

### 3. Model training with captured batch

- Status: working.
- Evidence: `train_model.py` trains a RandomForestClassifier and writes metrics to CSV.

### 4. Prediction after training

- Status: ready to run.
- Evidence: `predict_stream.py` is wired to the trained model and Kafka stream.

### 5. Full pipeline working in real time

- Status: mostly working.
- Evidence: producer -> Kafka -> Spark streaming -> Parquet -> model training works.
- Remaining item to verify in this session: live prediction stream after training.

## Exact commands by terminal (Windows PowerShell)

This section is copy/paste ready. Open **5 PowerShell terminals**.

Important:
- Run `cd C:\StreamMetoData\StreamMetoData\Meteorisk` in **every** terminal.
- Keep Terminal 1, 2, and 3 running while you observe Spark UI.
- Use `Ctrl+C` to stop long-running jobs.

## Terminal map

- **Terminal 1**: Kafka (Docker)
- **Terminal 2**: Spark History Server (persistent Spark UI at `http://localhost:18080`)
- **Terminal 3**: Spark Structured Streaming job (`streaming.py`)
- **Terminal 4**: Producer (`producer.py`)
- **Terminal 5**: Optional dashboard / training / prediction / tests

## One-time setup (run once in any terminal)

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
python -m venv venv
venv\Scripts\python.exe -m pip install -r requirements.txt
md data\spark_events
```

## Start everything for Spark UI (exact order)

### Terminal 1 - start Kafka

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
docker compose up -d
docker compose ps
```

Expected: Kafka container is `Up`.

### Terminal 2 - start Spark History Server

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
spark-class org.apache.spark.deploy.history.HistoryServer --logDirectory "data\spark_events"
```

Keep this terminal open.

### Terminal 3 - start Spark streaming (with event logging)

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=data\spark_events C:\StreamMetoData\StreamMetoData\Meteorisk\streaming.py
```

Keep this terminal open.

### Terminal 4 - start producer

Normal mode:

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
venv\Scripts\python.exe producer.py
```

Load-test mode (better for Spark UI metrics):

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
$env:LOAD_TEST_MODE = "true"
$env:LOAD_TEST_RATE = "5000"
$env:LOAD_TEST_DURATION = "300"
$env:LOAD_TEST_ANOMALY_PROB = "0.05"
venv\Scripts\python.exe producer.py
```

Keep this terminal open while collecting metrics.

## Exactly where to open Spark UI

- **Live Spark UI (only while a Spark job is running):** `http://localhost:4040`
- **Persistent Spark UI (recommended, remains after jobs finish):** `http://localhost:18080`

If `http://localhost:4040` is not available, check Spark logs for another port like `4041`.

## Terminal 5 - optional commands

### Run model training (with History Server logging)

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
spark-submit --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=data\spark_events C:\StreamMetoData\StreamMetoData\Meteorisk\train_model.py
```

### Run live prediction (with History Server logging)

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=data\spark_events C:\StreamMetoData\StreamMetoData\Meteorisk\predict_stream.py
```

### Run dashboard

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
streamlit run C:\StreamMetoData\StreamMetoData\Meteorisk\dashboard.py
```

Dashboard URL: `http://localhost:8501`

### Run tests

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
venv\Scripts\python.exe -m pytest tests\ -v
```

## Minimal mode (3 terminals, no History Server)

Use this only if you want a quick run and do not need persistent UI.

- Terminal 1:

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
docker compose up -d
```

- Terminal 2:

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 C:\StreamMetoData\StreamMetoData\Meteorisk\streaming.py
```

- Terminal 3:

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
venv\Scripts\python.exe producer.py
```

Spark UI in this mode: `http://localhost:4040` only while job is running.

## Stop commands (clean shutdown)

1. In producer terminal: `Ctrl+C`
2. In streaming terminal: `Ctrl+C`
3. In History Server terminal: `Ctrl+C`
4. In any terminal, stop Kafka:

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
docker compose down
```

## Spark UI quick checklist

1. Start Terminal 2 (History Server).
2. Run Spark jobs with both flags:

```powershell
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=data\spark_events
```

3. Open `http://localhost:18080`.
4. Click the finished application.
5. Capture `Jobs`, `Stages`, and `Streaming` tabs.

## What to capture for the rubric

### Spark UI screenshots (via History Server)

After each job finishes, go to `http://localhost:18080`, click the completed job, and capture:

- `Jobs` tab: total execution time for the job.
- `Stages` tab: shuffle read/write bytes, scheduler delay, spill to disk, and stage duration.
- `SQL` tab (if applicable): query duration and execution breakdown.
- `Streaming` tab: input rate (events/sec), processing rate, batch duration, and queue backlog if present.

**Tips for screenshots:**
- Use the History Server URL because the data persists.
- Run your jobs with the event logging flags enabled.
- Capture at least 3–5 batches or stages to get meaningful averages.

### Comparison table for the report

For the two architectures, record at least:

- Execution time.
- Shuffle time between stages.
- I/O per stage.
- Scheduler delay.
- Spill to disk.
- Streaming input rate and processing rate.

Suggested comparison axes:

- Local Windows + Docker Kafka + Spark Structured Streaming.
- Google Colab / notebook-based execution, if you run the notebook and collect the same measurements.

## Current status summary

- Working now: producer, Kafka, streaming, training, Streamlit dashboard, tests.
- Still missing for a perfect rubric submission: formal architecture comparison metrics, Spark UI screenshots, and a BI tool export outside Streamlit.
