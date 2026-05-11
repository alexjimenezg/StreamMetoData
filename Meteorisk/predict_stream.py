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


# ---------------------------------------------------------------------
# Features (MISMO orden que train_model.py)
# ---------------------------------------------------------------------
FEATURE_COLUMNS = [
    "temperature",
    "humidity",
    "precipitation",
    "wind_speed",
    "wind_gusts",
    "surface_pressure",
    "apparent_temperature",
    "weather_code",
]


# ---------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------
def create_spark_session():
    """Crea la SparkSession para la inferencia en streaming."""
    spark = (
        SparkSession.builder
        .appName("MeteoriskPredictStream")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------
# Schema del JSON publicado por producer.py
# ---------------------------------------------------------------------
def define_weather_schema():
    """Mismo schema que streaming.py."""
    return StructType([
        StructField("city",                 StringType(),  True),
        StructField("timestamp",            StringType(),  True),
        StructField("temperature",          DoubleType(),  True),
        StructField("humidity",             DoubleType(),  True),
        StructField("precipitation",        DoubleType(),  True),
        StructField("wind_speed",           DoubleType(),  True),
        StructField("wind_gusts",           DoubleType(),  True),
        StructField("surface_pressure",     DoubleType(),  True),
        StructField("apparent_temperature", DoubleType(),  True),
        StructField("weather_code",         IntegerType(), True),
    ])


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
def parse_weather_events(kafka_df, schema):
    """Aplica from_json y castea timestamp."""
    parsed_df = (
        kafka_df
        .select(from_json(col("json_str"), schema).alias("data"))
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
        print(f"[predict_stream] No se encontró el modelo en {config.MODEL_PATH}.")
        print("  Ejecuta primero:  spark-submit train_model.py")
        return None

    try:
        return RandomForestClassificationModel.load(config.MODEL_PATH)
    except Exception as exc:
        print(f"[predict_stream] Error al cargar el modelo: {exc}")
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
    print("[predict_stream] Iniciando consumidor de predicciones...")
    print(f"  Broker Kafka : {config.KAFKA_BOOTSTRAP_SERVERS}")
    print(f"  Tópico       : {config.KAFKA_TOPIC}")
    print(f"  Modelo       : {config.MODEL_PATH}")
    print(f"  Predicciones : {config.DATA_PREDICTIONS_PATH}")

    model = load_model()
    if model is None:
        sys.exit(1)
    print(
        f"[predict_stream] Modelo cargado "
        f"(numClasses={model.numClasses}, numFeatures={model.numFeatures})."
    )

    spark = create_spark_session()
    schema = define_weather_schema()

    kafka_df = read_kafka_stream(spark)
    parsed_df = parse_weather_events(kafka_df, schema)
    clean_df = clean_weather_events(parsed_df)
    feature_df = add_features(clean_df)
    predictions_df = predict_risk(model, feature_df)

    queries = [
        start_console_query(predictions_df),
        start_predictions_parquet_query(predictions_df),
    ]

    print(f"[predict_stream] {len(queries)} queries iniciadas. Esperando eventos... (Ctrl+C para salir)")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n[predict_stream] Interrupción recibida. Deteniendo queries...")
    finally:
        for q in queries:
            try:
                if q.isActive:
                    q.stop()
            except Exception as exc:
                print(f"[predict_stream] Error al detener una query: {exc}")
        spark.stop()
        print("[predict_stream] Spark detenido correctamente.")


if __name__ == "__main__":
    main()
