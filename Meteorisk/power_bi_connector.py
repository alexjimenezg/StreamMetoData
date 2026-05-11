"""
Real-time data connector for Power BI Push Dataset.

This script reads aggregated weather metrics from Spark streaming (or Parquet)
and posts them to a Power BI Streaming Dataset via REST API.

Setup:
1. Create a Power BI Streaming Dataset (see instructions below)
2. Set POWER_BI_DATASET_ID and POWER_BI_PUSH_URL environment variables
3. Run: python power_bi_connector.py
"""

import os
import json
import time
import requests
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==================== POWER BI CONFIGURATION ====================

# Option 1: Power BI Push Dataset (REST API)
# Get this URL from Power BI when you create a Streaming Dataset
POWER_BI_PUSH_URL = os.getenv(
    'POWER_BI_PUSH_URL',
    'https://api.powerbi.com/beta/workspaces/{workspace_id}/datasets/{dataset_id}/rows?key={key}'
)

# ==================== DATA SOURCE ====================

PARQUET_FOLDER = Path('data/aggregates')  # Where Spark writes aggregated data


def read_latest_metrics(max_retries=3):
    """Read latest aggregated metrics from Parquet folder."""
    try:
        if not PARQUET_FOLDER.exists():
            logger.warning(f"Parquet folder not found: {PARQUET_FOLDER}")
            return None
        
        parquet_files = list(PARQUET_FOLDER.glob('*.parquet'))
        if not parquet_files:
            logger.info("No parquet files yet")
            return None
        
        # Read latest file
        latest_file = max(parquet_files, key=os.path.getctime)
        df = pd.read_parquet(latest_file)
        
        # Return latest row
        if len(df) > 0:
            return df.iloc[-1].to_dict()
        return None
    except Exception as e:
        logger.error(f"Error reading metrics: {e}")
        return None


def format_for_power_bi(metric_dict):
    """Convert metric dict to Power BI compatible format."""
    if not metric_dict:
        return None
    
    # Extract window timestamps if present
    window_start = metric_dict.get('window_start', datetime.now())
    window_end = metric_dict.get('window_end', datetime.now())
    
    # Convert to ISO format for Power BI
    if hasattr(window_start, 'isoformat'):
        window_start = window_start.isoformat()
    if hasattr(window_end, 'isoformat'):
        window_end = window_end.isoformat()
    
    return {
        'Timestamp': datetime.now().isoformat(),
        'WindowStart': window_start,
        'WindowEnd': window_end,
        'City': metric_dict.get('city', 'Mexico City'),
        'AvgTemperature': float(metric_dict.get('avg_temperature', 0)),
        'MaxTemperature': float(metric_dict.get('max_temperature', 0)),
        'MinTemperature': float(metric_dict.get('min_temperature', 0)),
        'AvgHumidity': float(metric_dict.get('avg_humidity', 0)),
        'TotalPrecipitation': float(metric_dict.get('total_precipitation', 0)),
        'MaxWindSpeed': float(metric_dict.get('max_wind_speed', 0)),
        'MaxWindGusts': float(metric_dict.get('max_wind_gusts', 0)),
        'AvgSurfacePressure': float(metric_dict.get('avg_surface_pressure', 0)),
        'AvgApparentTemperature': float(metric_dict.get('avg_apparent_temperature', 0)),
        'EventCount': int(metric_dict.get('event_count', 0)),
    }


def post_to_power_bi(data, push_url=POWER_BI_PUSH_URL):
    """POST metrics to Power BI Streaming Dataset."""
    if not push_url or '{' in push_url:
        logger.warning("Power BI URL not configured. Set POWER_BI_PUSH_URL environment variable.")
        return False
    
    try:
        # Power BI expects rows as a list
        payload = {'rows': [data]}
        
        response = requests.post(
            push_url,
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            logger.info(f"✓ Posted to Power BI at {data['Timestamp']}")
            return True
        else:
            logger.error(f"Power BI error {response.status_code}: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Failed to post to Power BI: {e}")
        return False


def stream_to_power_bi(interval_seconds=5, duration_seconds=None):
    """Continuously stream metrics to Power BI."""
    logger.info(f"Starting Power BI stream (interval: {interval_seconds}s)")
    logger.info(f"Push URL: {POWER_BI_PUSH_URL[:80]}...")
    
    start_time = time.time()
    last_metric = None
    
    while True:
        # Check duration
        if duration_seconds and (time.time() - start_time) > duration_seconds:
            logger.info("Duration reached, stopping")
            break
        
        try:
            # Read latest metrics from Parquet
            metric_dict = read_latest_metrics()
            
            if metric_dict and metric_dict != last_metric:
                # Format for Power BI
                power_bi_data = format_for_power_bi(metric_dict)
                
                # Post to Power BI
                if post_to_power_bi(power_bi_data):
                    last_metric = metric_dict
            
            time.sleep(interval_seconds)
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
            break
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            time.sleep(interval_seconds)


if __name__ == '__main__':
    import sys
    
    if '--help' in sys.argv or '-h' in sys.argv:
        print(__doc__)
        sys.exit(0)
    
    # Parse arguments
    interval = int(os.getenv('INTERVAL_SECONDS', 5))
    duration = int(os.getenv('DURATION_SECONDS', 0)) or None
    
    print("\n" + "="*70)
    print("POWER BI REAL-TIME CONNECTOR")
    print("="*70)
    print(f"Reading from: {PARQUET_FOLDER}")
    print(f"Interval: {interval}s")
    if duration:
        print(f"Duration: {duration}s")
    print("="*70 + "\n")
    
    stream_to_power_bi(interval_seconds=interval, duration_seconds=duration)
