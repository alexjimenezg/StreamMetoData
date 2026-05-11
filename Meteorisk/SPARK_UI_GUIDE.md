# Spark UI Accessibility Guide

This guide explains how to keep the Spark UI accessible for capturing metrics in both **local** and **Colab** environments.

## Local (Windows + Docker Kafka + Spark)

### Setup Spark History Server (One-time)

The History Server preserves job metrics **after** they complete, so you can view them anytime.

```powershell
cd C:\StreamMetoData\StreamMetoData\Meteorisk

# Create event log directory
md data\spark_events

# Start History Server in a separate terminal (keep it running)
spark-class org.apache.spark.deploy.history.HistoryServer --logDirectory "data\spark_events"
```

The History Server will be available at: **http://localhost:18080**

### Enable Event Logging in spark-submit

Add these flags to capture metrics in the History Server:

```powershell
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=data\spark_events
```

### Example: Run streaming with History Server

```powershell
spark-submit `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 `
  --conf spark.eventLog.enabled=true `
  --conf spark.eventLog.dir=data\spark_events `
  C:\StreamMetoData\StreamMetoData\Meteorisk\streaming.py
```

### UI Access Timeline

| Phase | URL | Status |
|-------|-----|--------|
| **During job execution** | http://localhost:4040 | Live, real-time updates |
| **After job stops** | http://localhost:4040 | ❌ Closes immediately |
| **After job stops (with History)** | http://localhost:18080 | ✅ Persistent, archived |

### What to Screenshot

1. **Go to http://localhost:18080** (History Server)
2. **Click your completed job** in the list
3. **Capture these tabs:**
   - **Jobs**: Total execution time, number of tasks
   - **Stages**: Shuffle read/write bytes, scheduler delay, spill to disk
   - **SQL**: Query duration and plan breakdown (if using DataFrame API)
   - **Streaming** (if applicable): Input rate, processing rate, batch duration

### Example Screenshot Locations

```
http://localhost:18080/history/[APP_ID]/jobs/
http://localhost:18080/history/[APP_ID]/stages/
http://localhost:18080/history/[APP_ID]/SQL/
```

---

## Google Colab

### Event Logging (Enabled by Default)

The Colab notebook is configured with:

```python
.config('spark.eventLog.enabled', 'true')
.config('spark.eventLog.dir', EVENT_LOG_DIR)
```

Event logs are stored in `/content/meteorisk/spark_events/` and available throughout the notebook session.

### UI Access in Colab

**During execution:**
- The Spark driver UI is **not directly accessible** from a browser in Colab (it runs in a remote JVM).
- However, you can use the **Spark REST API** to fetch metrics programmatically.

**Metrics capture cells:**

The notebook includes a **section 5** ("Captura de métricas Spark UI") that:
- Connects to the local Spark REST API
- Extracts jobs, stages, executors, and environment info
- Saves metrics as JSON files: `colab_spark_jobs.json`, `colab_spark_stages.json`, etc.

### Key Metrics Extracted

From the Spark REST API, Colab captures:

- **Job duration**: Submission time to completion time
- **Shuffle bytes**: Total read and write
- **GC time**: Garbage collection milliseconds per stage
- **Executor stats**: Cores, memory, completed tasks, runtime, GC time
- **Host info**: CPU model, memory, number of processors

### Access Extracted Metrics

The metrics are saved in the notebook's `METRICS` folder and auto-downloaded as a ZIP file at the end:

```python
# File: /content/meteorisk_colab_metrics.zip
# Contains:
#   - colab_spark_jobs.json
#   - colab_spark_stages.json
#   - colab_spark_executors.json
#   - colab_spark_environment.json
#   - model_metrics.csv
```

### Comparing Metrics Across Runs

**Local metrics:**
1. View Spark UI at http://localhost:18080
2. Screenshot key tabs (Jobs, Stages, SQL, Streaming)
3. Record values: execution time, shuffle, GC, task count

**Colab metrics:**
1. Run the notebook
2. Download the metrics ZIP at the end
3. Open JSON files and record same values

**Create a comparison table:**

| Metric | Local | Colab | Difference |
|--------|-------|-------|-----------|
| Total execution time (s) | ... | ... | ... |
| Total shuffle read (MB) | ... | ... | ... |
| Total shuffle write (MB) | ... | ... | ... |
| Total GC time (ms) | ... | ... | ... |
| Number of tasks | ... | ... | ... |

---

## Tips for Best Results

### Local

1. **Keep History Server running** between jobs so you don't lose any data.
2. **Use the same flags** (`spark.eventLog.*`) for all jobs to ensure consistent logging.
3. **Take screenshots immediately** after a job completes while the UI is still rendering.
4. **Run with the same data volume** (e.g., same producer rate) to make comparisons fair.

### Colab

1. **Run the full notebook** to completion so all metrics cells execute.
2. **Save the downloaded ZIP** before running a new comparison.
3. **Compare JSON values** side-by-side using a spreadsheet or text editor.
4. **Use the same `RATE_EV_S`** and `STREAM_SECONDS` values across runs for consistency.

---

## Troubleshooting

### "History Server not found"

```
Error: The HistoryServer process is not running or not accessible at localhost:18080
```

**Fix:** Re-run the History Server startup command in a dedicated terminal and leave it running.

### "No events logged"

```
Error: Event logs directory is empty after job completes
```

**Fix:** Make sure you added the `spark.eventLog.*` flags to your spark-submit command.

### "Cannot connect to Spark UI in Colab"

This is expected. Use the REST API metrics capture instead (section 5 of the notebook).

---

## Quick Reference

### Local: Start fresh comparison

```powershell
# Terminal 1: History Server
spark-class org.apache.spark.deploy.history.HistoryServer --logDirectory "data\spark_events"

# Terminal 2: Run your jobs with logging
spark-submit --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=data\spark_events [...]

# View at: http://localhost:18080
```

### Colab: Run full pipeline

1. Run all cells in order
2. Metrics auto-saved to `/content/meteorisk/data/metrics/`
3. Download ZIP at the end
4. Extract and compare JSON files

---

## Related Files

- [run.md](run.md): Main runbook with exact commands
- [README.md](README.md): Project overview
- [Meteorisk_Colab.ipynb](Meteorisk_Colab.ipynb): Colab notebook with auto-metrics extraction
