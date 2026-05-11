# Real-Time Data Visualization: Power BI & Tableau Integration

This guide shows how to visualize live Meteorisk data in **Power BI** or **Tableau** with real-time updates.

## Quick Comparison

| Feature | Streamlit (Current) | Power BI | Tableau |
|---------|---------------------|----------|---------|
| **Real-time updates** | Live (5-10s) | Live (API push) | Live (refresh) |
| **Setup complexity** | Easy | Medium | Hard |
| **Cost** | Free | Free (basic) → $10/mo (Pro) | $70/mo |
| **Best for rubric** | ✅ Works now | ✅ Recommended | ⚠️ Overkill |
| **Local + cloud** | Local only | Hybrid | Hybrid |

---

## Option 1: Power BI Streaming Dataset (Recommended)

### Why Power BI?
- ✅ **Free** tier available (Power BI Desktop)
- ✅ **Native REST API** for real-time data push
- ✅ **Easy setup** (5-10 minutes)
- ✅ **Professional dashboards** for your rubric
- ✅ Can run Power BI Desktop locally on Windows

### Setup Steps

#### Step 1: Create Power BI Streaming Dataset

In **Power BI Desktop** or **Power BI Service**:

1. **Go to Settings** → **Premium** (if using Service)
2. **Create new dataset** → **Streaming dataset**
3. **Choose: Push API**
4. **Define columns** (see schema below):

```json
{
  "Timestamp": "datetime",
  "WindowStart": "datetime",
  "WindowEnd": "datetime",
  "City": "text",
  "AvgTemperature": "number",
  "MaxTemperature": "number",
  "MinTemperature": "number",
  "AvgHumidity": "number",
  "TotalPrecipitation": "number",
  "MaxWindSpeed": "number",
  "MaxWindGusts": "number",
  "AvgSurfacePressure": "number",
  "AvgApparentTemperature": "number",
  "EventCount": "number"
}
```

5. **Copy the Push URL** (looks like: `https://api.powerbi.com/beta/workspaces/...`)
6. **Save** the dataset

#### Step 2: Configure the Connector

Set environment variables (or edit `power_bi_connector.py`):

**Windows PowerShell:**

```powershell
$env:POWER_BI_PUSH_URL = "https://api.powerbi.com/beta/workspaces/{workspace_id}/datasets/{dataset_id}/rows?key={key}"
```

Or create a `.env` file:

```
POWER_BI_PUSH_URL=https://api.powerbi.com/beta/workspaces/...
INTERVAL_SECONDS=5
```

#### Step 3: Start Spark Streaming + Power BI Connector

In **Terminal 1** - Start Kafka:
```powershell
docker compose up -d
```

In **Terminal 2** - Start Spark streaming (writes to Parquet):
```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 C:\StreamMetoData\StreamMetoData\Meteorisk\streaming.py
```

In **Terminal 3** - Start producer:
```powershell
venv\Scripts\python.exe producer.py
```

In **Terminal 4** - Start Power BI connector:
```powershell
venv\Scripts\python.exe power_bi_connector.py
```

#### Step 4: View Live Data in Power BI

1. **Open Power BI Desktop** / **Power BI Service**
2. **Create a new report** using your Streaming Dataset
3. **Add visualizations**:
   - **Line chart**: Temperature over time
   - **Gauge**: Current humidity, wind speed
   - **Table**: Latest metrics
   - **Map**: City location (if you add geo data)
4. **Refresh rate**: Updates appear **every 5 seconds** (or your `INTERVAL_SECONDS`)

### Example Power BI Report Layout

```
┌─────────────────────────────────────────────┐
│         METEORISK REAL-TIME DASHBOARD      │
├─────────────────────────────────────────────┤
│  Current Temp: 24°C  │  Humidity: 65%      │
│  Wind Speed: 12 m/s  │  Precipitation: 2mm │
├─────────────────────────────────────────────┤
│                                             │
│  Temperature Trend (Last 60 min)            │
│  ╱╲                                         │
│ ╱  ╲  ╱╲                                    │
│╱    ╲╱  ╲                                   │
│                                             │
├─────────────────────────────────────────────┤
│  Latest Metrics Table                       │
│  Time  │ Avg Temp │ Max Humid │ Precip     │
│  14:00 │ 24.5°C   │ 68%       │ 0.2mm      │
│  14:05 │ 24.2°C   │ 66%       │ 0.1mm      │
│  14:10 │ 23.8°C   │ 64%       │ 0.0mm      │
└─────────────────────────────────────────────┘
```

---

## Option 2: Tableau Server + PostgreSQL Database

### Why?
- ✅ **Industry standard** for BI
- ✅ **Powerful clustering** and statistical analysis
- ⚠️ **Requires PostgreSQL** or similar database
- ⚠️ **More setup** than Power BI

### Setup Steps

#### Step 1: Create PostgreSQL Table

```sql
CREATE TABLE weather_metrics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    city VARCHAR(100),
    avg_temperature FLOAT,
    max_temperature FLOAT,
    min_temperature FLOAT,
    avg_humidity FLOAT,
    total_precipitation FLOAT,
    max_wind_speed FLOAT,
    max_wind_gusts FLOAT,
    avg_surface_pressure FLOAT,
    avg_apparent_temperature FLOAT,
    event_count INT
);

CREATE INDEX idx_timestamp ON weather_metrics(timestamp DESC);
```

#### Step 2: Modify `streaming.py` to Write to PostgreSQL

Add to your Spark streaming query (after window aggregation):

```python
from pyspark.sql.functions import to_timestamp

stats = (clean
    .withWatermark('timestamp', '2 minutes')
    .groupBy(window(col('timestamp'), '1 minute'), col('city'))
    .agg(...)
    .withColumn('window_start', col('window.start'))
    .withColumn('window_end', col('window.end'))
    .drop('window'))

# Write to PostgreSQL
(stats
    .writeStream
    .format('jdbc')
    .option('url', 'jdbc:postgresql://localhost:5432/meteorisk')
    .option('dbtable', 'weather_metrics')
    .option('user', 'postgres')
    .option('password', 'your_password')
    .option('checkpointLocation', 'data/checkpoints/postgres')
    .start())
```

#### Step 3: Connect Tableau to PostgreSQL

1. **Open Tableau**
2. **Data** → **New Data Source**
3. **Select PostgreSQL**
4. **Enter connection details**:
   - Server: `localhost`
   - Database: `meteorisk`
   - Username: `postgres`
5. **Select table**: `weather_metrics`
6. **Refresh live**: `Data` → `Refresh Rate` → Set to auto-refresh every 5 min

---

## Option 3: Kafka → Confluent Cloud → Power BI (Advanced)

### Why?
- ✅ **True real-time** (not polling)
- ✅ **Scalable** to thousands of events/sec
- ⚠️ **Requires Confluent subscription** (~$5/mo)
- ⚠️ **More complex setup**

### High-level flow:
```
Kafka (Docker) 
  → Kafka Connector (Confluent)
    → Power BI Push API
      → Dashboard
```

This requires Kafka Connect, not covered in detail here. See [Confluent Cloud Connectors](https://confluent.cloud).

---

## Option 4: REST API + Tableau Web Data Connector

### Why?
- ✅ **No database needed**
- ⚠️ **Requires creating a custom REST endpoint**
- ⚠️ **More code** than Parquet → Power BI

### Idea:
1. Create a simple Flask/FastAPI endpoint that returns the latest metrics as JSON
2. Tableau uses **Web Data Connector** to pull from it
3. Configure Tableau to refresh every 5 seconds

Example Flask endpoint:

```python
from flask import Flask, jsonify
import pandas as pd
from pathlib import Path

app = Flask(__name__)

@app.route('/api/metrics/latest', methods=['GET'])
def get_latest_metrics():
    parquet_folder = Path('data/aggregates')
    parquet_files = list(parquet_folder.glob('*.parquet'))
    if not parquet_files:
        return jsonify([])
    
    latest_file = max(parquet_files, key=os.path.getctime)
    df = pd.read_parquet(latest_file)
    
    return jsonify(df.tail(10).to_dict(orient='records'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
```

Then in Tableau: **Data** → **Web Data Connector** → `http://localhost:5000/api/metrics/latest`

---

## Which Option Should You Choose?

### **For your rubric (2 puntos for BI tool):**

1. **Best: Power BI Desktop + Streaming Dataset** ⭐⭐⭐
   - Easiest to set up (5 min)
   - Free
   - Looks professional
   - **Use `power_bi_connector.py` script provided**

2. **Good: Tableau Server + PostgreSQL** ⭐⭐
   - More professional appearance
   - Requires database
   - ~20 min setup

3. **Acceptable: Keep Streamlit + add Power BI**
   - You already have Streamlit working
   - Add Power BI alongside it
   - Shows you know multiple tools

4. **Not recommended: REST API + Web Connector**
   - Too complex for the rubric effort
   - Unstable for real-time

### **Recommendation:**
✅ **Use Option 1 (Power BI Streaming)** — it's the sweet spot:
- Takes 10 minutes to set up
- Looks professional (meets rubric 2-point requirement)
- Works locally on Windows
- Truly real-time updates
- Can screenshot for your report

---

## Running Everything Together

### Step-by-step to see live Power BI data:

**Terminal 1: Kafka**
```powershell
docker compose up -d
docker compose ps
```

**Terminal 2: Spark Streaming**
```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 C:\StreamMetoData\StreamMetoData\Meteorisk\streaming.py
```

**Terminal 3: Producer**
```powershell
$env:LOAD_TEST_MODE = "true"
$env:LOAD_TEST_RATE = "1000"
$env:LOAD_TEST_DURATION = "300"
venv\Scripts\python.exe producer.py
```

**Terminal 4: Power BI Connector**
```powershell
$env:POWER_BI_PUSH_URL = "https://api.powerbi.com/beta/workspaces/.../rows?key=..."
venv\Scripts\python.exe power_bi_connector.py
```

**Terminal 5: Power BI Desktop**
- Open Power BI Desktop
- Create report from Streaming Dataset
- Watch live updates every 5 seconds

---

## Troubleshooting

### "Power BI URL not configured"
```
Error: Power BI URL not configured. Set POWER_BI_PUSH_URL environment variable.
```
**Fix:** Get your push URL from Power BI → Streaming Dataset → API settings

### "No data appearing in Power BI"
**Checklist:**
1. ✓ Spark streaming is running (check `data/aggregates/` folder)
2. ✓ `power_bi_connector.py` is running (should show "✓ Posted to Power BI")
3. ✓ Parquet files exist and have data
4. ✓ Power BI report is using the correct Streaming Dataset

### "Connection refused" to Kafka
**Fix:** Ensure `docker compose up -d` is running

---

## Deliverables for Your Rubric

To satisfy rubric item **"Indicadores del tráfico en tiempo real (2 ptos)"**:

1. **Screenshot of Power BI dashboard** showing live metrics
2. **Screenshot of Spark UI** showing streaming throughput
3. **Screenshot or video** of metrics updating in real-time
4. **Brief note** explaining the architecture: "Data flows from Kafka → Spark Streaming → Parquet → Power BI API → Real-time Dashboard"

---

## Next Steps

1. **Create Power BI Streaming Dataset** (5 min)
2. **Get the Push URL** and set `POWER_BI_PUSH_URL` environment variable
3. **Run the 4-terminal setup** above
4. **Create a Power BI report** with your desired visualizations
5. **Screenshot** for your rubric submission

---

## Files Included

- `power_bi_connector.py`: Script to stream Parquet metrics to Power BI
- `SPARK_UI_GUIDE.md`: Guide for capturing Spark metrics
- `run.md`: Full runbook with commands
- `Meteorisk_Colab.ipynb`: Colab version (also supports metrics export)

See [power_bi_connector.py](power_bi_connector.py) for the complete implementation.
