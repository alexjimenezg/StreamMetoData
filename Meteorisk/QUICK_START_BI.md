# Quick Start: Live Data to Power BI or Tableau

## TL;DR (5-minute setup)

Want to see your streaming data in **Power BI** or **Tableau** right now?

### Option A: Power BI (Easiest) ⭐

**Prerequisite:** Power BI Desktop (free) or Power BI Service account (free tier)

1. **Create Streaming Dataset** in Power BI:
   - Go to Power BI → Create → Streaming dataset
   - Choose **Push API**
   - Define columns (copy from [REAL_TIME_BI_SETUP.md](REAL_TIME_BI_SETUP.md))
   - **Copy the Push URL**

2. **Configure environment variable:**
   ```powershell
   $env:POWER_BI_PUSH_URL = "https://api.powerbi.com/beta/workspaces/..."
   ```

3. **Start the pipeline:**
   ```powershell
   # Terminal 1
   docker compose up -d
   
   # Terminal 2
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 `
     C:\StreamMetoData\StreamMetoData\Meteorisk\streaming.py
   
   # Terminal 3
   venv\Scripts\python.exe producer.py
   
   # Terminal 4 (NEW)
   venv\Scripts\python.exe power_bi_connector.py
   ```

4. **Create report in Power BI:**
   - Use your Streaming Dataset
   - Add visualizations (line chart, gauge, table)
   - Watch data update **every 5 seconds**

**Done!** 🎉

---

### Option B: REST API → Any BI Tool (Alternative)

If you don't have Power BI or want to use Tableau:

1. **Start the HTTP API server:**
   ```powershell
   pip install -r requirements-bi.txt
   venv\Scripts\python.exe http_api_server.py
   ```

2. **Access the API:**
   ```
   http://localhost:5000/api/metrics/latest
   ```

3. **In Power BI:**
   - Get Data → Web → `http://localhost:5000/api/metrics/latest`

4. **In Tableau:**
   - Data → New Data Source → Web Data Connector
   - URL: `http://localhost:5000/api/metrics/latest`
   - Refresh every 5-10 seconds

---

## What You'll See

### Power BI Live Dashboard

```
┌─────────────────────────────────────┐
│   METEORISK LIVE DASHBOARD         │
├─────────────────────────────────────┤
│  Temp: 24°C ▌════════════░          │
│  Humidity: 65% ▌════════════░       │
│  Wind: 12 m/s ▌══════░              │
│  Precipitation: 2mm ▌═░             │
├─────────────────────────────────────┤
│                                     │
│  Temperature Trend                  │
│    24.5 ┤                 ╱╲        │
│    24.0 ┤             ╱╲╱  ╲       │
│    23.5 ┤         ╱╲╱      ╲      │
│    23.0 └─────────────────────      │
│         14:00 14:05 14:10 14:15    │
│                                     │
│  Latest Events                      │
│  Time    Temp  Humidity Precip     │
│  14:10   23.8  64%      0.0mm      │
│  14:05   24.2  66%      0.1mm      │
│  14:00   24.5  68%      0.2mm      │
└─────────────────────────────────────┘
```

**Updates live!** Every 5 seconds, new data appears in Power BI.

---

## Complete Setup (All Options)

### Pre-requisites

```powershell
# Install pip dependencies
pip install -r requirements-bi.txt

# Ensure Kafka and Spark are available
docker version
spark-submit --version
```

### Full 5-Terminal Setup

**Terminal 1: Kafka**
```powershell
docker compose up -d; docker compose ps
```

**Terminal 2: Spark Streaming** (reads Kafka → writes Parquet)
```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 `
  C:\StreamMetoData\StreamMetoData\Meteorisk\streaming.py
```

**Terminal 3: Producer** (generates test data)
```powershell
$env:LOAD_TEST_MODE = "true"
$env:LOAD_TEST_RATE = "1000"
venv\Scripts\python.exe producer.py
```

**Terminal 4: Power BI Connector OR HTTP API**

Option A (Power BI):
```powershell
$env:POWER_BI_PUSH_URL = "https://api.powerbi.com/beta/workspaces/..."
venv\Scripts\python.exe power_bi_connector.py
```

Option B (REST API for any BI tool):
```powershell
venv\Scripts\python.exe http_api_server.py
```

**Terminal 5: BI Tool**
- Power BI Desktop (open and create report)
- Tableau Server (connect to REST API)
- Or just open browser: `http://localhost:5000/api/metrics/latest`

---

## How Data Flows

### Power BI Route
```
Kafka (Docker)
  ↓ (spark-sql-kafka)
Spark Streaming
  ↓ (writes every minute)
Parquet Files (data/aggregates/)
  ↓ (reads Parquet)
power_bi_connector.py
  ↓ (HTTP POST)
Power BI Streaming Dataset
  ↓ (renders)
Power BI Dashboard 📊
```

### HTTP API Route
```
Kafka (Docker)
  ↓
Spark Streaming
  ↓
Parquet Files
  ↓ (reads on demand)
http_api_server.py (Flask)
  ↓ (HTTP GET)
Power BI / Tableau
  ↓
Live Dashboard 📊
```

---

## Configuration

### Power BI Connector (`power_bi_connector.py`)

Environment variables:
```powershell
$env:POWER_BI_PUSH_URL = "https://api.powerbi.com/..."
$env:INTERVAL_SECONDS = "5"        # How often to push (default 5)
$env:DURATION_SECONDS = "600"      # How long to run (optional)
```

### HTTP API Server (`http_api_server.py`)

Access at:
- API Docs: `http://localhost:5000/api/docs`
- Latest data: `http://localhost:5000/api/metrics/latest?limit=20`
- Summary: `http://localhost:5000/api/metrics/summary`
- CSV export: `http://localhost:5000/api/metrics/csv`

---

## Troubleshooting

### "Power BI connection failed"
- ✓ Make sure `POWER_BI_PUSH_URL` is set correctly
- ✓ Check Power BI Streaming Dataset exists
- ✓ Verify Spark streaming is writing Parquet files

### "No data in Power BI"
- ✓ Check `data/aggregates/` folder exists and has `.parquet` files
- ✓ Run `power_bi_connector.py` and watch for "✓ Posted to Power BI" messages
- ✓ Refresh Power BI report (Ctrl+R)

### "HTTP API returns empty data"
- ✓ Start Spark streaming first (should write to `data/aggregates/`)
- ✓ Wait 1-2 minutes for Parquet files to appear
- ✓ Check `http://localhost:5000/api/health` for diagnostics

### "Spark streaming not writing Parquet"
- ✓ Ensure Kafka is running: `docker compose ps`
- ✓ Ensure producer is sending data: Check terminal 3 output
- ✓ Check `data/processed/` folder exists

---

## Metrics Captured

Each data point includes:
- **Timestamp**: When the metric was generated
- **WindowStart / WindowEnd**: Spark window boundaries
- **City**: Mexico City
- **AvgTemperature, MaxTemperature, MinTemperature**
- **AvgHumidity**
- **TotalPrecipitation**
- **MaxWindSpeed, MaxWindGusts**
- **AvgSurfacePressure, AvgApparentTemperature**
- **EventCount**: Number of events in the window

---

## For Your Rubric (2 Puntos)

To satisfy **"Indicadores del tráfico en tiempo real (estadística y/o clustering)"**:

1. ✅ Create **Real-time dashboard** (Power BI or Tableau)
2. ✅ Show **live metrics** (temperature, humidity, wind, precipitation)
3. ✅ Include **aggregation** (1-minute windows shown in dashboard)
4. ✅ **Screenshot** the live dashboard
5. ✅ Include in your report with caption:
   > *"Real-time weather metrics dashboard showing aggregated data updated every 5 seconds, connected via Power BI Streaming Dataset REST API"*

---

## Next Steps

1. **Pick your tool:**
   - Power BI Desktop (free) → Use `power_bi_connector.py`
   - Tableau Server → Use `http_api_server.py`
   - Both → Run both scripts

2. **Follow the TL;DR** at the top of this guide

3. **Run the 5-terminal setup**

4. **Take a screenshot** for your rubric

5. **Done!** You now have a live BI dashboard 🎉

---

## More Info

- **[REAL_TIME_BI_SETUP.md](REAL_TIME_BI_SETUP.md)** - Detailed setup guide
- **[SPARK_UI_GUIDE.md](SPARK_UI_GUIDE.md)** - Capturing Spark metrics
- **[run.md](run.md)** - Full runbook with all commands
- **[power_bi_connector.py](power_bi_connector.py)** - Source code
- **[http_api_server.py](http_api_server.py)** - REST API server

---

**Questions?** See the troubleshooting section above or check the detailed guides.

**Ready?** Start with the TL;DR above! ⚡
