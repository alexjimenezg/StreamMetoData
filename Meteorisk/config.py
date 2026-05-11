"""
config.py
---------
Constantes globales del proyecto Meteorisk.

Aquí se centralizan los parámetros usados por todos los módulos
(producer, streaming, train_model, predict_stream y dashboard).
De esta forma, si cambia el broker de Kafka, la ciudad o las rutas,
basta con modificarlo en un solo lugar.
"""

from pathlib import Path
import os

# Get the root directory (where this config.py is located)
PROJECT_ROOT = Path(__file__).parent

# Convert to string for compatibility with all tools
def _path_str(p: Path) -> str:
    """Convert Path to forward-slash string for cross-platform compatibility."""
    return str(p).replace("\\", "/")

# ---------------------------------------------------------------------
# Configuración de Kafka
# ---------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "weather_stream"

# Environment variable override
if os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

# ---------------------------------------------------------------------
# Configuración de la ciudad / fuente de datos
# ---------------------------------------------------------------------
CITY_NAME = "Mexico City"
LATITUDE = 19.4326
LONGITUDE = -99.1332

# Override from environment
if os.getenv("CITY_NAME"):
    CITY_NAME = os.getenv("CITY_NAME")

# ---------------------------------------------------------------------
# Rutas de almacenamiento en disco (using pathlib for Windows compatibility)
# ---------------------------------------------------------------------
DATA_RAW_PATH = _path_str(PROJECT_ROOT / "data" / "raw")
DATA_PROCESSED_PATH = _path_str(PROJECT_ROOT / "data" / "processed")
DATA_AGGREGATES_PATH = _path_str(PROJECT_ROOT / "data" / "aggregates")
DATA_PREDICTIONS_PATH = _path_str(PROJECT_ROOT / "data" / "predictions")
DATA_METRICS_PATH = _path_str(PROJECT_ROOT / "data" / "metrics")
CHECKPOINTS_PATH = _path_str(PROJECT_ROOT / "data" / "checkpoints")

# Create directories if they don't exist
for path in [DATA_RAW_PATH, DATA_PROCESSED_PATH, DATA_AGGREGATES_PATH, 
             DATA_PREDICTIONS_PATH, DATA_METRICS_PATH, CHECKPOINTS_PATH]:
    Path(path).mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------
# Modelo entrenado
# ---------------------------------------------------------------------
MODEL_PATH = _path_str(PROJECT_ROOT / "models" / "weather_risk_model")

# Create model directory if it doesn't exist
Path(MODEL_PATH).mkdir(parents=True, exist_ok=True)
