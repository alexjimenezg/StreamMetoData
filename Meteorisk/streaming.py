"""
streaming.py
------------
Consumidor de eventos meteorológicos con Spark Structured Streaming.

Funcionalidad:
  1. Lee eventos desde Kafka (tópico config.KAFKA_TOPIC).
  2. Parsea el JSON con un schema manual.
  3. Convierte timestamp a tipo timestamp y limpia registros inválidos.
  4. Sale a 4 destinos en paralelo:
       a) Consola con eventos limpios (append).
       b) Parquet con eventos procesados      -> data/processed/
       c) Consola con agregados por ventana   (complete).
       d) Parquet con agregados por ventana   -> data/aggregates/ (append + watermark)

Ejecución sugerida:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 streaming.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    to_timestamp,
    window,
)

import config
from utils.logging_config import get_logger
from utils.spark_utils import create_spark_session, stop_spark_session
from utils.schema_registry import get_weather_schema, get_required_columns

logger = get_logger(__name__)


# ---------------------------------------------------------------------
# Lectura desde Kafka
# ---------------------------------------------------------------------
def read_kafka_stream(spark):
    """
    Lee el tópico Kafka como un DataFrame en streaming.
    La columna `value` viene en binario; la convertimos a string.
    """
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
# Parseo del JSON
# ---------------------------------------------------------------------
def parse_weather_events(kafka_df, schema):
    """
    Aplica from_json sobre la cadena JSON y expande los campos en
    columnas. Convierte `timestamp` a tipo timestamp de Spark.
    """
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
    """Elimina registros con campos críticos nulos."""
    required_columns = get_required_columns()

    clean_df = parsed_df.dropna(subset=required_columns)

    return clean_df.select(
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
    )


# ---------------------------------------------------------------------
# Estadísticas por ventana
# ---------------------------------------------------------------------
def create_windowed_statistics(clean_df):
    """
    Calcula estadísticas dinámicas en ventanas de 1 minuto agrupadas
    por ciudad. Se aplica watermark de 2 minutos para que el sink
    parquet pueda usar outputMode("append").

    Devuelve un DataFrame con:
        city, window_start, window_end,
        avg_temperature, max_temperature, min_temperature,
        avg_humidity, total_precipitation,
        max_wind_speed, max_wind_gusts,
        avg_surface_pressure, avg_apparent_temperature,
        event_count
    """
    watermarked_df = clean_df.withWatermark("timestamp", "2 minutes")

    stats_df = (
        watermarked_df
        .groupBy(
            window(col("timestamp"), "1 minute"),
            col("city"),
        )
        .agg(
            avg("temperature").alias("avg_temperature"),
            spark_max("temperature").alias("max_temperature"),
            spark_min("temperature").alias("min_temperature"),
            avg("humidity").alias("avg_humidity"),
            spark_sum("precipitation").alias("total_precipitation"),
            spark_max("wind_speed").alias("max_wind_speed"),
            spark_max("wind_gusts").alias("max_wind_gusts"),
            avg("surface_pressure").alias("avg_surface_pressure"),
            avg("apparent_temperature").alias("avg_apparent_temperature"),
            count("*").alias("event_count"),
        )
    )

    # Aplanamos la columna struct `window` para que el parquet sea
    # más cómodo de leer luego desde pandas / dashboard.
    stats_df = (
        stats_df
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end",   col("window.end"))
        .select(
            "city",
            "window_start",
            "window_end",
            "avg_temperature",
            "max_temperature",
            "min_temperature",
            "avg_humidity",
            "total_precipitation",
            "max_wind_speed",
            "max_wind_gusts",
            "avg_surface_pressure",
            "avg_apparent_temperature",
            "event_count",
        )
    )

    return stats_df


# ---------------------------------------------------------------------
# Sinks
# ---------------------------------------------------------------------
def start_console_query(clean_df):
    """Eventos limpios -> consola (append)."""
    return (
        clean_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .start()
    )


def start_processed_parquet_query(clean_df):
    """Eventos limpios -> parquet en data/processed/ (append)."""
    return (
        clean_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", config.DATA_PROCESSED_PATH)
        .option("checkpointLocation", config.CHECKPOINTS_PATH + "/processed")
        .start()
    )


def start_aggregates_console_query(stats_df):
    """Agregados por ventana -> consola (complete)."""
    return (
        stats_df.writeStream
        .format("console")
        .outputMode("complete")
        .option("truncate", "false")
        .start()
    )


def start_aggregates_parquet_query(stats_df):
    """
    Agregados por ventana -> parquet en data/aggregates/.

    Usamos outputMode("append") porque ya hay watermark aplicado en
    create_windowed_statistics; Spark emitirá una ventana cuando
    quede definitivamente cerrada por el watermark.
    """
    return (
        stats_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", config.DATA_AGGREGATES_PATH)
        .option("checkpointLocation", config.CHECKPOINTS_PATH + "/aggregates")
        .start()
    )


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    logger.info("Iniciando consumidor Spark Structured Streaming...")
    logger.info(f"  Broker Kafka : {config.KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"  Tópico       : {config.KAFKA_TOPIC}")
    logger.info(f"  Procesados   : {config.DATA_PROCESSED_PATH}")
    logger.info(f"  Agregados    : {config.DATA_AGGREGATES_PATH}")
    logger.info(f"  Checkpoints  : {config.CHECKPOINTS_PATH}")

    spark = create_spark_session("MeteoriskStreaming")
    schema = get_weather_schema()

    try:
        kafka_df = read_kafka_stream(spark)
        parsed_df = parse_weather_events(kafka_df, schema)
        clean_df = clean_weather_events(parsed_df)
        stats_df = create_windowed_statistics(clean_df)

        # Cada query con su propio checkpoint -> sin conflictos.
        queries = [
            start_console_query(clean_df),
            start_processed_parquet_query(clean_df),
            start_aggregates_console_query(stats_df),
            start_aggregates_parquet_query(stats_df),
        ]

        logger.info(f"{len(queries)} queries iniciadas. Esperando eventos... (Ctrl+C para salir)")

        # awaitAnyTermination devuelve cuando cualquiera de las queries
        # termina (por error o por orden externa).
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Interrupción recibida. Deteniendo queries...")
    except Exception as exc:
        logger.error(f"Error en streaming: {exc}", exc_info=True)
    finally:
        try:
            for q in spark.streams.active:
                try:
                    if q.isActive:
                        q.stop()
                except Exception as exc:
                    logger.warning(f"Error al detener query: {exc}")
        except Exception as exc:
            logger.warning(f"Error iterando queries: {exc}")
        finally:
            stop_spark_session(spark)
            logger.info("Spark detenido correctamente.")


if __name__ == "__main__":
    main()
