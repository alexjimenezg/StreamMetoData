# 📊 Meteorisk Real-Time BI Integration Summary

## What's New

You can now visualize **live streaming weather data** in **Power BI** or **Tableau** with real-time updates.

### Files Added

| File | Purpose |
|------|---------|
| **[QUICK_START_BI.md](QUICK_START_BI.md)** | ⭐ **START HERE** - 5-minute setup guide |
| **[REAL_TIME_BI_SETUP.md](REAL_TIME_BI_SETUP.md)** | Detailed setup for Power BI, Tableau, Kafka, REST API |
| **power_bi_connector.py** | Script to stream Parquet → Power BI Push Dataset |
| **http_api_server.py** | REST API server for any BI tool to consume |
| **requirements-bi.txt** | Python packages for BI tools (Flask, requests, etc.) |

---

## Quick Summary

### Option 1: Power BI (Recommended) ⭐⭐⭐

```powershell
# 1. Create Streaming Dataset in Power BI
# 2. Get the Push URL and set it as env var
$env:POWER_BI_PUSH_URL = "https://api.powerbi.com/..."

# 3. Start the pipeline (4 terminals)
docker compose up -d                    # Terminal 1: Kafka
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 streaming.py  # T2
venv\Scripts\python.exe producer.py     # T3
venv\Scripts\python.exe power_bi_connector.py  # T4 (NEW)

# 4. Create report in Power BI Desktop
# → Live metrics update every 5 seconds
# → Screenshot for your rubric ✓
```

**Pros:**
- ✅ Free (Power BI Desktop)
- ✅ Professional look
- ✅ 5-minute setup
- ✅ Truly real-time (5-second updates)

---

### Option 2: REST API (Flexible) ⭐⭐

```powershell
# 1. Start HTTP server
pip install -r requirements-bi.txt
venv\Scripts\python.exe http_api_server.py

# 2. Access data at
http://localhost:5000/api/metrics/latest

# 3. Connect Power BI or Tableau to this URL
# → Get Data → Web → http://localhost:5000/api/metrics/latest
```

**Pros:**
- ✅ Works with any BI tool
- ✅ No special setup in Power BI/Tableau
- ✅ Can download CSV anytime
- ✅ Good for testing

---

## How It Works

### Data Flow

```
Producer (synthetic weather data)
  ↓
Kafka (Docker container)
  ↓
Spark Structured Streaming
  ↓ (aggregates per minute)
Parquet Files (data/aggregates/)
  ↓
Power BI Connector  OR  HTTP API Server
  ↓
Power BI Push Dataset  OR  REST API
  ↓
Live Dashboard
```

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    LOCAL WINDOWS PC                      │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  ┌──────────┐      ┌─────────┐      ┌────────────┐    │
│  │ Producer │ ───→ │  Kafka  │ ───→ │   Spark    │    │
│  │ (Open-   │      │(Docker) │      │ Streaming  │    │
│  │  Meteo)  │      └─────────┘      └────────────┘    │
│  └──────────┘                             │             │
│                                           ↓             │
│                                  ┌────────────────┐    │
│                                  │   Parquet      │    │
│                                  │   (1-min agg)  │    │
│                                  └────────────────┘    │
│                                           │             │
│                  ┌────────────────────────┼──────────┐  │
│                  ↓                        ↓          │  │
│          ┌──────────────┐       ┌──────────────┐   │  │
│          │  Power BI    │       │ HTTP API     │   │  │
│          │  Connector   │       │ Server       │   │  │
│          └──────────────┘       └──────────────┘   │  │
│                  │                        │          │  │
└──────────────────┼────────────────────────┼──────────┘
                   │                        │
                   ↓                        ↓
           ┌──────────────┐       ┌──────────────┐
           │   Power BI   │       │  Tableau /   │
           │   Desktop    │       │  Power BI    │
           │   (local)    │       │  (remote)    │
           └──────────────┘       └──────────────┘
                   ↓                        ↓
              LIVE DASHBOARD  ←  Updates every 5-10 sec
```

---

## Rubric Coverage

### Item 2: "Indicadores del tráfico en tiempo real (2 ptos)"

With this setup, you get:

✅ **Real-time metrics** - Temperature, humidity, wind, precipitation updated every 5 seconds  
✅ **Aggregation/Statistics** - 1-minute windows showing min/max/avg values  
✅ **Professional BI tool** - Power BI or Tableau (not just Streamlit)  
✅ **Live dashboard** - Screenshot for your report  
✅ **Traffic indicators** - Event count, performance metrics shown in real-time  

**How to document for rubric:**

1. **Screenshot**: Power BI or Tableau dashboard with live metrics
2. **Caption**: "Real-time weather metrics dashboard via Power BI Streaming Dataset / Tableau REST API"
3. **Explanation**: "Data flows from Kafka → Spark Streaming → Parquet → BI Tool (5-10 second refresh)"

---

## Installation

### 1. Install BI dependencies

```powershell
pip install -r requirements-bi.txt
```

This adds:
- `flask` - HTTP API server
- `flask-cors` - Cross-origin support
- `requests` - For posting to Power BI

### 2. Power BI Setup

1. Install **Power BI Desktop** (free from Microsoft)
2. Create a **Streaming Dataset**:
   - New → Streaming dataset
   - Choose API
   - Define columns (temperature, humidity, etc.)
   - Copy the Push URL
3. Set environment variable:
   ```powershell
   $env:POWER_BI_PUSH_URL = "https://api.powerbi.com/..."
   ```

### 3. Run the Pipeline

See [QUICK_START_BI.md](QUICK_START_BI.md) for detailed instructions.

---

## Testing

### Test HTTP API

```powershell
# Start the server
venv\Scripts\python.exe http_api_server.py

# In another terminal, test the endpoints
curl http://localhost:5000/api/metrics/latest
curl http://localhost:5000/api/metrics/summary
curl http://localhost:5000/api/health
```

### Test Power BI Connector

```powershell
# Check logs
venv\Scripts\python.exe power_bi_connector.py

# Should show:
# INFO - Reading from: data/aggregates
# INFO - ✓ Posted to Power BI at 2024-05-11T14:30:45.123456
```

---

## Troubleshooting

### Power BI shows "No data"

1. ✓ Check `data/aggregates/` folder has `.parquet` files
2. ✓ Verify `streaming.py` is running
3. ✓ Check `power_bi_connector.py` logs show "✓ Posted"
4. ✓ Refresh Power BI report (Ctrl+R)

### HTTP API returns empty

1. ✓ Wait 2 minutes for Spark to write first Parquet file
2. ✓ Check `http://localhost:5000/api/health` for diagnostics
3. ✓ Verify `PARQUET_FOLDER` path in `http_api_server.py`

### "Connection refused" to Kafka

```powershell
docker compose ps
# Should show kafka running. If not:
docker compose up -d
```

---

## Performance Tips

### For Better Real-Time Updates

1. **Reduce Spark window size:**
   ```python
   # In streaming.py, change from 1 minute to 30 seconds
   .groupBy(window(col('timestamp'), '30 seconds'), col('city'))
   ```

2. **Increase producer rate:**
   ```powershell
   $env:LOAD_TEST_RATE = "5000"  # 5000 events/sec
   ```

3. **Reduce Power BI push interval:**
   ```powershell
   $env:INTERVAL_SECONDS = "2"   # Every 2 seconds
   ```

### For More Data

1. **Keep producer running longer:**
   ```powershell
   $env:LOAD_TEST_DURATION = "3600"  # 1 hour
   ```

2. **Create a historical report:**
   ```powershell
   # Download all data as CSV
   curl http://localhost:5000/api/metrics/csv > metrics.csv
   ```

---

## Next Steps

1. **Read [QUICK_START_BI.md](QUICK_START_BI.md)** for step-by-step setup
2. **Choose your tool** (Power BI recommended)
3. **Run the 4-terminal setup**
4. **Create your dashboard**
5. **Screenshot for your rubric**
6. **Done!** ✓

---

## Related Documentation

- **[SPARK_UI_GUIDE.md](SPARK_UI_GUIDE.md)** - Capturing Spark metrics for comparison
- **[REAL_TIME_BI_SETUP.md](REAL_TIME_BI_SETUP.md)** - Advanced BI setup options
- **[run.md](run.md)** - Full runbook with all commands
- **[README.md](README.md)** - Project overview

---

**Questions?** See the troubleshooting sections or detailed guides above.

**Ready to go?** 🚀 Start with [QUICK_START_BI.md](QUICK_START_BI.md)!
