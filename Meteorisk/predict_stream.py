"""
predict_stream.py
-----------------
Clasificación en tiempo real de eventos meteorológicos.

Flujo:
  1. Carga el modelo RandomForest entrenado por train_model.py.
  2. Lee eventos desde el tópico Kafka `weather_stream`.
  3. Parsea el JSON, limpia nulos y arma el vector `features`
     respetando exactamente el mismo orden usado en el entrenamiento.
  4. Aplica el modelo al stream y traduce la predicción numérica a
     un texto interpretativo ("normal" / "moderate" / "critical").
  5. Salida a dos destinos en paralelo:
       a) Consola (append).
       b) Parquet en data/predictions/ (append + checkpoint propio).

Ejecución sugerida:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 predict_stream.py
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, when
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassificationModel

import config
from utils.logging_config import setup_logging
from utils.spark_utils import create_spark_session
from utils.schema_registry import get_weather_schema, get_feature_columns

logger = setup_logging(__name__)


# Feature columns centralized in utils.schema_registry
FEATURE_COLUMNS = get_feature_columns()


# Spark session factory imported from utils


# Weather schema centralized in utils.schema_registry
SCHEMA = get_weather_schema()


# ---------------------------------------------------------------------
# Lectura desde Kafka
# ---------------------------------------------------------------------
def read_kafka_stream(spark):
    """Lee el tópico Kafka como stream y convierte value a string."""
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    return kafka_df.selectExpr("CAST(value AS STRING) AS json_str")


# ---------------------------------------------------------------------
# Parseo
# ---------------------------------------------------------------------
def parse_weather_events(kafka_df):
    """Aplica from_json y castea timestamp."""
    parsed_df = (
        kafka_df
        .select(from_json(col("json_str"), SCHEMA).alias("data"))
        .select("data.*")
    )

    parsed_df = parsed_df.withColumn(
        "timestamp",
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm"),
    )
    return parsed_df


# ---------------------------------------------------------------------
# Limpieza
# ---------------------------------------------------------------------
def clean_weather_events(parsed_df):
    """Elimina registros con cualquier columna crítica en null."""
    required_columns = [
        "city",
        "timestamp",
        "temperature",
        "humidity",
        "precipitation",
        "wind_speed",
        "wind_gusts",
        "surface_pressure",
        "apparent_temperature",
        "weather_code",
    ]
    return parsed_df.dropna(subset=required_columns)


# ---------------------------------------------------------------------
# Features
# ---------------------------------------------------------------------
def add_features(clean_df):
    """Construye la columna `features` con VectorAssembler."""
    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features",
    )
    return assembler.transform(clean_df)


# ---------------------------------------------------------------------
# Modelo
# ---------------------------------------------------------------------
def load_model():
    """
    Carga el modelo entrenado desde config.MODEL_PATH.
    Si no existe, devuelve None para que main() lo maneje.
    """
    if not os.path.isdir(config.MODEL_PATH):
        logger.error(f"No se encontró el modelo en {config.MODEL_PATH}.")
        logger.error("  Ejecuta primero:  spark-submit train_model.py")
        return None

    try:
        return RandomForestClassificationModel.load(config.MODEL_PATH)
    except Exception as exc:
        logger.error(f"Error al cargar el modelo: {exc}")
        return None


# ---------------------------------------------------------------------
# Predicción
# ---------------------------------------------------------------------
def predict_risk(model, feature_df):
    """
    Aplica el modelo al stream y mapea la predicción numérica a un
    texto interpretativo. Devuelve solo las columnas relevantes.
    """
    predictions = model.transform(feature_df)

    predictions = predictions.withColumn(
        "risk_prediction",
        when(col("prediction") == 0.0, "normal")
        .when(col("prediction") == 1.0, "moderate")
        .when(col("prediction") == 2.0, "critical")
        .otherwise("unknown"),
    )

    return predictions.select(
        "city",
        "timestamp",
        "temperature",
        "humidity",
        "precipitation",
        "wind_speed",
        "wind_gusts",
        "surface_pressure",
        "apparent_temperature",
        "weather_code",
        "prediction",
        "risk_prediction",
    )


# ---------------------------------------------------------------------
# Sinks
# ---------------------------------------------------------------------
def start_console_query(predictions_df):
    """Predicciones -> consola (append)."""
    return (
        predictions_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .start()
    )


def start_predictions_parquet_query(predictions_df):
    """Predicciones -> parquet en data/predictions/ (append)."""
    return (
        predictions_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", config.DATA_PREDICTIONS_PATH)
        .option("checkpointLocation", config.CHECKPOINTS_PATH + "/predictions")
        .start()
    )


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    try:
        logger.info("Iniciando consumidor de predicciones...")
        logger.info(f"  Broker Kafka : {config.KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"  Tópico       : {config.KAFKA_TOPIC}")
        logger.info(f"  Modelo       : {config.MODEL_PATH}")
        logger.info(f"  Predicciones : {config.DATA_PREDICTIONS_PATH}")

        spark = create_spark_session("MeteoriskPredictStream")

        model = load_model()
        if model is None:
            logger.error("No se pudo cargar el modelo. Abortando.")
            spark.stop()
            sys.exit(1)
        logger.info("Modelo cargado "
            f"(numClasses={model.numClasses}, numFeatures={model.numFeatures})."
        )

        kafka_df = read_kafka_stream(spark)
        parsed_df = parse_weather_events(kafka_df)
        clean_df = clean_weather_events(parsed_df)
        feature_df = add_features(clean_df)
        predictions_df = predict_risk(model, feature_df)

        queries = [
            start_console_query(predictions_df),
            start_predictions_parquet_query(predictions_df),
        ]

        logger.info(f"{len(queries)} queries iniciadas. Esperando eventos... (Ctrl+C para salir)")

        try:
            spark.streams.awaitAnyTermination()
        except KeyboardInterrupt:
            logger.info("Interrupción recibida. Deteniendo queries...")
        finally:
            for q in queries:
                try:
                    if q.isActive:
                        q.stop()
                except Exception as exc:
                    logger.error(f"Error al detener una query: {exc}")
            spark.stop()
            logger.info("Spark detenido correctamente.")
    
    except Exception as exc:
        logger.exception(f"Error fatal: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
