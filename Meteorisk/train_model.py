"""
train_model.py
--------------
Entrenamiento del modelo de clasificación de riesgo meteorológico.

Flujo:
  1. Lee los datos procesados desde data/processed/.
  2. Limpia nulos en las columnas usadas como features.
  3. Crea la variable objetivo risk_label (0=normal, 1=moderate, 2=critical)
     usando reglas simples sobre temperatura, viento y precipitación.
  4. Construye la columna features con VectorAssembler.
  5. Entrena un RandomForestClassifier sencillo.
  6. Evalúa accuracy, precision, recall y f1.
  7. Guarda el modelo en models/weather_risk_model y las métricas
     en data/metrics/model_metrics.csv.

Ejecución:
    spark-submit train_model.py
"""

import csv
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

import config


# ---------------------------------------------------------------------
# Columnas usadas como features (ORDEN IMPORTANTE: predict_stream.py
# debe usar el mismo orden).
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

METRICS_FILE = os.path.join(config.DATA_METRICS_PATH, "model_metrics.csv")


# ---------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------
def create_spark_session():
    """Crea la SparkSession para el entrenamiento."""
    spark = (
        SparkSession.builder
        .appName("MeteoriskTrainModel")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------
# Lectura
# ---------------------------------------------------------------------
def load_processed_data(spark):
    """Lee los Parquet procesados generados por streaming.py."""
    if not os.path.isdir(config.DATA_PROCESSED_PATH):
        print(f"[train_model] No existe la carpeta {config.DATA_PROCESSED_PATH}.")
        return None

    try:
        df = spark.read.parquet(config.DATA_PROCESSED_PATH)
    except Exception as exc:
        print(f"[train_model] Error al leer Parquet: {exc}")
        return None

    return df


# ---------------------------------------------------------------------
# Limpieza
# ---------------------------------------------------------------------
def clean_training_data(df):
    """Quita registros con nulos en columnas requeridas para entrenar."""
    return df.dropna(subset=FEATURE_COLUMNS)


# ---------------------------------------------------------------------
# Etiquetado de riesgo
# ---------------------------------------------------------------------
def add_risk_labels(df):
    """
    Agrega dos columnas:
      - risk_label : 0=normal, 1=moderate, 2=critical (numérica)
      - risk_level : texto interpretable
    Regla: "critical" tiene prioridad sobre "moderate".
    """
    df = df.withColumn(
        "risk_label",
        when(
            (col("temperature") > 35)
            | (col("wind_speed") > 60)
            | (col("precipitation") > 50),
            2,
        )
        .when(
            ((col("temperature") >= 30) & (col("temperature") <= 35))
            | ((col("precipitation") >= 20) & (col("precipitation") <= 50))
            | ((col("wind_speed") >= 40) & (col("wind_speed") <= 60)),
            1,
        )
        .otherwise(0),
    )

    df = df.withColumn(
        "risk_level",
        when(col("risk_label") == 2, "critical")
        .when(col("risk_label") == 1, "moderate")
        .otherwise("normal"),
    )

    return df


# ---------------------------------------------------------------------
# Features
# ---------------------------------------------------------------------
def prepare_features(df):
    """Construye la columna `features` con VectorAssembler."""
    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features",
    )
    return assembler.transform(df)


# ---------------------------------------------------------------------
# Entrenamiento
# ---------------------------------------------------------------------
def train_random_forest(train_df):
    """Entrena un RandomForestClassifier simple."""
    rf = RandomForestClassifier(
        labelCol="risk_label",
        featuresCol="features",
        numTrees=30,
        maxDepth=5,
        seed=42,
    )
    return rf.fit(train_df)


# ---------------------------------------------------------------------
# Evaluación
# ---------------------------------------------------------------------
def evaluate_model(model, test_df):
    """Calcula accuracy, weightedPrecision, weightedRecall y f1."""
    predictions = model.transform(test_df)

    metrics = {}
    for metric_name in ["accuracy", "weightedPrecision", "weightedRecall", "f1"]:
        evaluator = MulticlassClassificationEvaluator(
            labelCol="risk_label",
            predictionCol="prediction",
            metricName=metric_name,
        )
        metrics[metric_name] = evaluator.evaluate(predictions)

    return metrics, predictions


# ---------------------------------------------------------------------
# Persistencia del modelo
# ---------------------------------------------------------------------
def save_model(model):
    """Guarda el modelo en config.MODEL_PATH (sobrescribe si existe)."""
    try:
        model.write().overwrite().save(config.MODEL_PATH)
        print(f"[train_model] Modelo guardado en {config.MODEL_PATH}")
    except Exception as exc:
        print(f"[train_model] Error al guardar el modelo: {exc}")


# ---------------------------------------------------------------------
# Métricas en CSV
# ---------------------------------------------------------------------
def save_metrics(metrics_dict):
    """Guarda las métricas en data/metrics/model_metrics.csv."""
    os.makedirs(config.DATA_METRICS_PATH, exist_ok=True)
    try:
        with open(METRICS_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["metric", "value"])
            for key, value in metrics_dict.items():
                writer.writerow([key, value])
        print(f"[train_model] Métricas guardadas en {METRICS_FILE}")
    except Exception as exc:
        print(f"[train_model] Error al guardar métricas: {exc}")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    print("[train_model] Iniciando entrenamiento...")
    spark = create_spark_session()

    # 1) Cargar datos
    df = load_processed_data(spark)
    if df is None:
        print("[train_model] No se pudieron leer datos procesados. Abortando.")
        spark.stop()
        sys.exit(1)

    total_rows = df.count()
    print(f"[train_model] Total de registros leídos: {total_rows}")
    if total_rows == 0:
        print("[train_model] data/processed/ está vacío. Ejecuta producer.py + streaming.py primero.")
        spark.stop()
        sys.exit(1)

    # 2) Limpieza
    df_clean = clean_training_data(df)
    clean_rows = df_clean.count()
    print(f"[train_model] Registros tras limpieza: {clean_rows}")

    if clean_rows < 20:
        print("[train_model] Muy pocos registros para entrenar (<20). Abortando.")
        spark.stop()
        sys.exit(1)

    # 3) Etiquetado
    df_labeled = add_risk_labels(df_clean)
    print("[train_model] Distribución de risk_level:")
    df_labeled.groupBy("risk_level").count().orderBy("risk_level").show()

    distinct_classes = df_labeled.select("risk_label").distinct().count()
    if distinct_classes < 2:
        print(
            "[train_model] ADVERTENCIA: el dataset contiene una sola clase de riesgo. "
            "El modelo no podrá aprender a discriminar. "
            "Activa DEMO_MODE en producer.py para inyectar anomalías y reintenta."
        )

    # 4) Features
    df_features = prepare_features(df_labeled)

    # 5) Split
    train_df, test_df = df_features.randomSplit([0.8, 0.2], seed=42)
    train_rows = train_df.count()
    test_rows = test_df.count()
    print(f"[train_model] Tamaño train: {train_rows} | Tamaño test: {test_rows}")

    if train_rows == 0 or test_rows == 0:
        print("[train_model] Split insuficiente, no se puede entrenar/evaluar. Abortando.")
        spark.stop()
        sys.exit(1)

    # 6) Entrenamiento
    print("[train_model] Entrenando RandomForestClassifier...")
    model = train_random_forest(train_df)

    # 7) Evaluación
    metrics, predictions = evaluate_model(model, test_df)
    print("[train_model] Métricas del modelo:")
    for k, v in metrics.items():
        print(f"  {k:>20s} = {v:.4f}")

    print("[train_model] Algunas predicciones de ejemplo:")
    (
        predictions
        .select(
            "timestamp",
            "temperature",
            "precipitation",
            "wind_speed",
            "risk_level",
            "risk_label",
            "prediction",
        )
        .show(10, truncate=False)
    )

    # 8) Guardar modelo
    save_model(model)

    # 9) Guardar métricas
    metrics_to_save = dict(metrics)
    metrics_to_save["total_rows"] = total_rows
    metrics_to_save["clean_rows"] = clean_rows
    save_metrics(metrics_to_save)

    spark.stop()
    print("[train_model] Listo.")


if __name__ == "__main__":
    main()
