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

Use the Kafka package and the full file path:

```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 C:\StreamMetoData\StreamMetoData\Meteorisk\streaming.py
```

While this job runs, open Spark UI:

```text
http://localhost:4040
```

### 4) Train the model

```powershell
spark-submit C:\StreamMetoData\StreamMetoData\Meteorisk\train_model.py
```

This creates:

- `models\weather_risk_model_<timestamp>`
- `models\weather_risk_model`
- `data\metrics\model_metrics.csv`

### 5) Run live prediction

```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 C:\StreamMetoData\StreamMetoData\Meteorisk\predict_stream.py
```

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

## What to capture for the rubric

### Spark UI screenshots

Capture these while `streaming.py` or `predict_stream.py` is running:

- `Jobs` tab: total execution time.
- `Stages` tab: shuffle read/write, scheduler delay, spill, and stage duration.
- `SQL` tab: query duration and execution breakdown.
- `Streaming` tab: input rate, processing rate, batch duration, and queue backlog if present.

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
