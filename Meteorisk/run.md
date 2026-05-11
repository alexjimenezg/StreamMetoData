# Meteorisk Runbook

This file is the quick run guide for the rubric. It tells you what already works, what is still missing, and the exact commands to run on Windows PowerShell.

Important: in PowerShell use `;` between commands, not `&&`.

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

## Exact commands

Run everything from this folder:

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk
```

### 1) One-time setup

If the virtual environment is not ready yet:

```powershell
python -m venv venv
venv\Scripts\python.exe -m pip install -r requirements.txt
```

Start Kafka:

```powershell
docker compose up -d
docker compose ps
```

**Optional: Start Spark History Server** (keeps UI accessible after jobs finish):

Create a directory for Spark event logs:

```powershell
md data\spark_events
```

Start the History Server in a separate terminal:

```powershell
spark-class org.apache.spark.deploy.history.HistoryServer --logDirectory "data\spark_events"
```

The History Server will be available at `http://localhost:18080`. Then add this flag to all spark-submit commands:

```powershell
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=data\spark_events
```

### 2) Generate data

Normal near-real-time mode:

```powershell
venv\Scripts\python.exe producer.py
```

High-volume load test mode for Spark UI measurements:

```powershell
$env:LOAD_TEST_MODE = "true"
$env:LOAD_TEST_RATE = "5000"
$env:LOAD_TEST_DURATION = "60"
$env:LOAD_TEST_ANOMALY_PROB = "0.05"
venv\Scripts\python.exe producer.py
```

To stop the producer, press `Ctrl+C`.

### 3) Run Spark Structured Streaming

Use the Kafka package and the full file path. **With History Server** (add event logging):

```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=data\spark_events C:\StreamMetoData\StreamMetoData\Meteorisk\streaming.py
```

**Without History Server** (simple, temporary UI):

```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 C:\StreamMetoData\StreamMetoData\Meteorisk\streaming.py
```

While this job runs, open the **live Spark UI**:

```text
http://localhost:4040
```

This job will run continuously. Press `Ctrl+C` to stop it, but the UI will close immediately. If you ran with History Server enabled, the job will be archived and visible at `http://localhost:18080` even after it stops.

### 4) Train the model

**With History Server** (recommended for capturing metrics):

```powershell
spark-submit --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=data\spark_events C:\StreamMetoData\StreamMetoData\Meteorisk\train_model.py
```

**Without History Server**:

```powershell
spark-submit C:\StreamMetoData\StreamMetoData\Meteorisk\train_model.py
```

This creates:

- `models\weather_risk_model_<timestamp>`
- `models\weather_risk_model`
- `data\metrics\model_metrics.csv`

After training finishes, the live UI at `http://localhost:4040` will close. But if History Server is running, view the completed job at `http://localhost:18080`.

### 5) Run live prediction

**With History Server**:

```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=data\spark_events C:\StreamMetoData\StreamMetoData\Meteorisk\predict_stream.py
```

**Without History Server**:

```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 C:\StreamMetoData\StreamMetoData\Meteorisk\predict_stream.py
```

This runs continuously until you press `Ctrl+C`. The live UI is available at `http://localhost:4040`.

### 6) Run the dashboard

```powershell
streamlit run C:\StreamMetoData\StreamMetoData\Meteorisk\dashboard.py
```

The dashboard usually opens at:

```text
http://localhost:8501
```

### 7) Run tests

Unit + integration tests:

```powershell
venv\Scripts\python.exe -m pytest tests\ -v
```

Unit only:

```powershell
venv\Scripts\python.exe -m pytest tests\unit -v
```

Integration only:

```powershell
venv\Scripts\python.exe -m pytest tests\integration -v
```

## UI Accessibility Guide

For detailed instructions on keeping Spark UIs accessible in both local and Colab environments, see [SPARK_UI_GUIDE.md](SPARK_UI_GUIDE.md).

**TL;DR:**
- **Local:** Start Spark History Server (`spark-class org.apache.spark.deploy.history.HistoryServer --logDirectory "data\\spark_events"`), then use `--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=data\\spark_events` flags.
- **Colab:** Event logging enabled by default; metrics auto-extracted and downloaded as JSON.
- **Access local UI:** http://localhost:18080 (after job completes with History Server)
- **Access Colab metrics:** Download ZIP at end of notebook, extract JSON files.

## UI Accessibility Guide

### During job execution (live UI)

- Open `http://localhost:4040` while a Spark job is running.
- The UI shows **real-time** metrics: executors, tasks, stages, and shuffle activity.
- This UI closes immediately when the job stops.

### After job execution (History Server)

- If you started the History Server (see section 1), the UI is at `http://localhost:18080`.
- All completed jobs are preserved here even after they finish.
- The History Server remains open until you stop it.
- This is where you should capture screenshots for your report, because the data will not disappear.

### How to keep the UI accessible

1. **Start the History Server once** (in the setup section).
2. **Add event logging flags** to your spark-submit commands (shown above).
3. **Run your jobs** - each one will be logged and visible in the History Server.
4. **Screenshot the History Server** at `http://localhost:18080` after jobs complete.

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
