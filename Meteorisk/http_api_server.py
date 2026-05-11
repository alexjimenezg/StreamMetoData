"""
Lightweight Real-Time HTTP API Server for Tableau, Power BI, or any BI tool.

This serves live weather metrics as JSON via a REST API.
Tableau or Power BI can poll this endpoint for real-time data.

Usage:
    python http_api_server.py

The server will be available at:
    http://localhost:5000/api/metrics/latest    (latest 10 rows)
    http://localhost:5000/api/metrics/all        (all rows)
    http://localhost:5000/api/metrics/summary    (summary stats)

Then in Power BI: Get Data → Web → paste the URL
Or in Tableau: Data → New Data Source → Web Data Connector
"""

import os
import logging
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import json
from flask import Flask, jsonify, request
from flask_cors import CORS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # Enable cross-origin requests

PARQUET_FOLDER = Path('data/aggregates')
CACHE_SECONDS = 2  # Cache to avoid reading disk too frequently
LAST_CACHE = {'time': None, 'data': None}


def get_cached_metrics():
    """Read metrics from Parquet with caching."""
    now = datetime.now()
    
    # Return cached data if fresh
    if (LAST_CACHE['time'] and 
        (now - LAST_CACHE['time']).total_seconds() < CACHE_SECONDS and
        LAST_CACHE['data'] is not None):
        return LAST_CACHE['data']
    
    try:
        if not PARQUET_FOLDER.exists():
            return None
        
        parquet_files = list(PARQUET_FOLDER.glob('*.parquet'))
        if not parquet_files:
            return None
        
        # Read all parquet files and concatenate
        dfs = []
        for f in parquet_files[-5:]:  # Last 5 files to avoid reading all
            try:
                df = pd.read_parquet(f)
                dfs.append(df)
            except:
                pass
        
        if not dfs:
            return None
        
        df = pd.concat(dfs, ignore_index=True)
        
        # Sort by timestamp and keep last 1000 rows
        if 'window_start' in df.columns:
            df = df.sort_values('window_start', ascending=False).head(1000)
        
        # Cache the result
        LAST_CACHE['time'] = now
        LAST_CACHE['data'] = df
        
        return df
    except Exception as e:
        logger.error(f"Error reading metrics: {e}")
        return None


def format_response(df, limit=None):
    """Format DataFrame for JSON response."""
    if df is None or len(df) == 0:
        return []
    
    if limit:
        df = df.head(limit)
    
    # Convert timestamps to strings
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str)
    
    return df.to_dict(orient='records')


# ==================== API ENDPOINTS ====================

@app.route('/api/metrics/latest', methods=['GET'])
def latest_metrics():
    """Get latest N metrics (default 10)."""
    limit = request.args.get('limit', 10, type=int)
    
    df = get_cached_metrics()
    data = format_response(df, limit=limit)
    
    return jsonify({
        'timestamp': datetime.now().isoformat(),
        'count': len(data),
        'data': data
    })


@app.route('/api/metrics/all', methods=['GET'])
def all_metrics():
    """Get all cached metrics."""
    df = get_cached_metrics()
    data = format_response(df)
    
    return jsonify({
        'timestamp': datetime.now().isoformat(),
        'count': len(data),
        'data': data
    })


@app.route('/api/metrics/summary', methods=['GET'])
def summary_stats():
    """Get summary statistics of current metrics."""
    df = get_cached_metrics()
    
    if df is None or len(df) == 0:
        return jsonify({'error': 'No data available'})
    
    # Calculate statistics
    numeric_cols = df.select_dtypes(include=['number']).columns
    summary = {}
    
    for col in numeric_cols:
        summary[col] = {
            'current': float(df[col].iloc[-1]) if len(df) > 0 else None,
            'avg': float(df[col].mean()),
            'min': float(df[col].min()),
            'max': float(df[col].max()),
            'std': float(df[col].std())
        }
    
    return jsonify({
        'timestamp': datetime.now().isoformat(),
        'rows': len(df),
        'summary': summary
    })


@app.route('/api/metrics/csv', methods=['GET'])
def metrics_csv():
    """Download metrics as CSV."""
    df = get_cached_metrics()
    
    if df is None:
        return jsonify({'error': 'No data available'})
    
    csv_data = df.to_csv(index=False)
    
    return csv_data, 200, {
        'Content-Disposition': 'attachment; filename=meteorisk_metrics.csv',
        'Content-Type': 'text/csv'
    }


@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint."""
    df = get_cached_metrics()
    has_data = df is not None and len(df) > 0
    
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'data_available': has_data,
        'parquet_folder': str(PARQUET_FOLDER),
        'parquet_folder_exists': PARQUET_FOLDER.exists()
    })


@app.route('/api/docs', methods=['GET'])
def docs():
    """API documentation."""
    return jsonify({
        'title': 'Meteorisk Real-Time API',
        'description': 'REST API for accessing live weather metrics',
        'endpoints': {
            '/api/metrics/latest': {
                'method': 'GET',
                'description': 'Get latest N metrics',
                'params': {'limit': 'integer (default 10)'},
                'example': '/api/metrics/latest?limit=20'
            },
            '/api/metrics/all': {
                'method': 'GET',
                'description': 'Get all cached metrics',
                'example': '/api/metrics/all'
            },
            '/api/metrics/summary': {
                'method': 'GET',
                'description': 'Get summary statistics',
                'example': '/api/metrics/summary'
            },
            '/api/metrics/csv': {
                'method': 'GET',
                'description': 'Download metrics as CSV',
                'example': '/api/metrics/csv'
            },
            '/api/health': {
                'method': 'GET',
                'description': 'Health check',
                'example': '/api/health'
            }
        }
    })


@app.route('/', methods=['GET'])
def root():
    """Root endpoint with link to docs."""
    return jsonify({
        'message': 'Meteorisk Real-Time API Server',
        'docs': '/api/docs',
        'latest_data': '/api/metrics/latest'
    })


# ==================== ERROR HANDLERS ====================

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found. See /api/docs'}), 404


@app.errorhandler(500)
def server_error(error):
    logger.error(f"Server error: {error}")
    return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    print("\n" + "="*70)
    print("METEORISK REAL-TIME HTTP API SERVER")
    print("="*70)
    print(f"Reading from: {PARQUET_FOLDER}")
    print()
    print("Available endpoints:")
    print("  • http://localhost:5000/api/metrics/latest   (latest 10)")
    print("  • http://localhost:5000/api/metrics/all       (all data)")
    print("  • http://localhost:5000/api/metrics/summary   (statistics)")
    print("  • http://localhost:5000/api/metrics/csv       (download CSV)")
    print("  • http://localhost:5000/api/docs              (API documentation)")
    print()
    print("For Power BI: Get Data → Web → http://localhost:5000/api/metrics/latest")
    print("For Tableau:  Data → Web Data Connector → http://localhost:5000/api/metrics/latest")
    print("="*70 + "\n")
    
    app.run(host='0.0.0.0', port=5000, debug=False)
