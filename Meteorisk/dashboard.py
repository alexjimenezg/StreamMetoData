"""
dashboard.py
------------
Dashboard interactivo de Meteorisk con Streamlit.

Lee datos desde:
  - data/processed/    (eventos limpios)
  - data/aggregates/   (estadísticas por ventana)
  - data/predictions/  (predicciones del modelo)
  - data/metrics/model_metrics.csv (métricas del modelo)

Ejecución:
    streamlit run dashboard.py
"""

import os
import time

import pandas as pd
import plotly.express as px
import streamlit as st

import config
from utils.logging_config import setup_logging

logger = setup_logging(__name__)


# ---------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------
MAX_CHART_ROWS = 500           # Para no saturar las gráficas
MODEL_METRICS_FILE = os.path.join(config.DATA_METRICS_PATH, "model_metrics.csv")
AUTO_REFRESH_SECONDS = 10      # Refresco automático si el usuario lo activa


# ---------------------------------------------------------------------
# Carga de datos
# ---------------------------------------------------------------------
def load_parquet_folder(path):
    """
    Lee todos los Parquet de una carpeta. Devuelve DataFrame vacío si
    la carpeta no existe, está vacía o hay un error de lectura
    (p. ej. archivo en escritura).
    """
    if not os.path.isdir(path):
        logger.debug(f"Directorio no existe: {path}")
        return pd.DataFrame()

    try:
        df = pd.read_parquet(path)
        logger.debug(f"Cargados {len(df)} registros de {path}")
        return df
    except Exception as exc:
        logger.error(f"Error al leer {path}: {exc}")
        st.sidebar.warning(f"Error al leer {path}: {exc}")
        return pd.DataFrame()


def load_model_metrics(path):
    """Carga el CSV de métricas del modelo. DataFrame vacío si no existe."""
    if not os.path.isfile(path):
        logger.debug(f"Archivo de métricas no existe: {path}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        logger.debug(f"Cargadas métricas de {path}")
        return df
    except Exception as exc:
        logger.error(f"Error al leer {path}: {exc}")
        st.sidebar.warning(f"Error al leer {path}: {exc}")
        return pd.DataFrame()


# ---------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------
def show_sidebar_status(processed_df, aggregates_df, predictions_df, metrics_df):
    """Muestra el estado de carga de cada dataset y el botón de refresh."""
    st.sidebar.header("Estado de los datos")

    datasets = [
        ("Procesados",  processed_df,    config.DATA_PROCESSED_PATH),
        ("Agregados",   aggregates_df,   config.DATA_AGGREGATES_PATH),
        ("Predicciones", predictions_df, config.DATA_PREDICTIONS_PATH),
        ("Métricas",    metrics_df,      MODEL_METRICS_FILE),
    ]

    for name, df, path in datasets:
        if df.empty:
            st.sidebar.error(f"{name}: sin datos\n`{path}`")
        else:
            st.sidebar.success(f"{name}: {len(df):,} filas\n`{path}`")

    st.sidebar.markdown("---")
    auto_refresh = st.sidebar.checkbox(
        f"Auto-refresh cada {AUTO_REFRESH_SECONDS}s", value=False
    )
    refresh_clicked = st.sidebar.button("🔄 Actualizar datos")
    return auto_refresh, refresh_clicked


# ---------------------------------------------------------------------
# Métricas principales
# ---------------------------------------------------------------------
def show_main_metrics(processed_df):
    """KPIs en la parte superior del dashboard."""
    st.subheader("Resumen meteorológico")

    if processed_df.empty:
        st.info("Aún no hay datos procesados. Ejecuta `producer.py` + `streaming.py`.")
        return

    avg_temp = processed_df["temperature"].mean()
    avg_humidity = processed_df["humidity"].mean()
    total_precip = processed_df["precipitation"].sum()
    max_wind = processed_df["wind_speed"].max()
    total_events = len(processed_df)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Temperatura promedio (°C)", f"{avg_temp:.1f}")
    c2.metric("Humedad promedio (%)",      f"{avg_humidity:.1f}")
    c3.metric("Precipitación total (mm)",  f"{total_precip:.1f}")
    c4.metric("Viento máximo (km/h)",      f"{max_wind:.1f}")
    c5.metric("Eventos totales",           f"{total_events:,}")


# ---------------------------------------------------------------------
# Alertas
# ---------------------------------------------------------------------
def show_alerts(processed_df, predictions_df):
    """
    Muestra una alerta si en los datos recientes hay condiciones
    críticas o si el modelo predice "critical".
    """
    critical_in_data = False
    if not processed_df.empty:
        recent = processed_df.tail(50)
        critical_in_data = (
            (recent["temperature"] > 35).any()
            or (recent["wind_speed"] > 60).any()
            or (recent["precipitation"] > 50).any()
        )

    critical_in_pred = False
    if not predictions_df.empty and "risk_prediction" in predictions_df.columns:
        recent_pred = predictions_df.tail(50)
        critical_in_pred = (recent_pred["risk_prediction"] == "critical").any()

    if critical_in_data or critical_in_pred:
        st.error("⚠️ Riesgo meteorológico crítico detectado")
    else:
        st.success("✅ Condiciones meteorológicas dentro de rangos normales")


# ---------------------------------------------------------------------
# Gráficas de datos crudos
# ---------------------------------------------------------------------
def show_weather_charts(processed_df):
    """Series temporales de temperatura, humedad, precipitación y viento."""
    st.subheader("Series temporales (eventos procesados)")

    if processed_df.empty:
        st.info("No hay eventos procesados para graficar.")
        return

    df = (
        processed_df
        .dropna(subset=["timestamp"])
        .sort_values("timestamp")
        .tail(MAX_CHART_ROWS)
    )

    c1, c2 = st.columns(2)
    with c1:
        fig = px.line(df, x="timestamp", y="temperature", title="Temperatura (°C)")
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        fig = px.line(df, x="timestamp", y="humidity", title="Humedad (%)")
        st.plotly_chart(fig, use_container_width=True)

    c3, c4 = st.columns(2)
    with c3:
        fig = px.bar(df, x="timestamp", y="precipitation", title="Precipitación (mm)")
        st.plotly_chart(fig, use_container_width=True)
    with c4:
        fig = px.line(df, x="timestamp", y="wind_speed", title="Velocidad del viento (km/h)")
        st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------
# Agregados por ventana
# ---------------------------------------------------------------------
def show_aggregate_charts(aggregates_df):
    """Gráficas a partir de los agregados por ventana."""
    st.subheader("Agregados por ventana")

    if aggregates_df.empty:
        st.info("Aún no hay agregados disponibles (espera unos minutos al stream).")
        return

    df = (
        aggregates_df
        .dropna(subset=["window_start"])
        .sort_values("window_start")
        .tail(MAX_CHART_ROWS)
    )

    c1, c2, c3 = st.columns(3)
    with c1:
        fig = px.line(df, x="window_start", y="avg_temperature", title="Temperatura promedio")
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        fig = px.bar(df, x="window_start", y="total_precipitation", title="Precipitación total")
        st.plotly_chart(fig, use_container_width=True)
    with c3:
        fig = px.line(df, x="window_start", y="max_wind_speed", title="Viento máximo")
        st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------
# Predicciones
# ---------------------------------------------------------------------
def show_predictions(predictions_df):
    """Distribución de risk_prediction y tabla de últimas 20 predicciones."""
    st.subheader("Predicciones del modelo")

    if predictions_df.empty:
        st.info("Aún no hay predicciones. Ejecuta `predict_stream.py`.")
        return

    if "risk_prediction" in predictions_df.columns:
        counts = (
            predictions_df["risk_prediction"]
            .value_counts()
            .reindex(["normal", "moderate", "critical"], fill_value=0)
            .reset_index()
        )
        counts.columns = ["risk_prediction", "count"]

        fig = px.bar(
            counts,
            x="risk_prediction",
            y="count",
            color="risk_prediction",
            color_discrete_map={
                "normal":   "green",
                "moderate": "orange",
                "critical": "red",
            },
            title="Distribución de niveles de riesgo predichos",
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("**Últimas 20 predicciones**")
    cols_to_show = [
        "timestamp",
        "temperature",
        "precipitation",
        "wind_speed",
        "prediction",
        "risk_prediction",
    ]
    cols_present = [c for c in cols_to_show if c in predictions_df.columns]

    last_20 = (
        predictions_df
        .dropna(subset=["timestamp"])
        .sort_values("timestamp", ascending=False)
        .head(20)[cols_present]
    )
    st.dataframe(last_20, use_container_width=True)


# ---------------------------------------------------------------------
# Métricas del modelo
# ---------------------------------------------------------------------
def show_model_metrics(metrics_df):
    """Métricas del modelo entrenado (accuracy, F1, etc.)."""
    st.subheader("Métricas del modelo")

    if metrics_df.empty:
        st.info("No hay métricas. Ejecuta `train_model.py` para generarlas.")
        return

    metrics = dict(zip(metrics_df["metric"], metrics_df["value"]))

    def fmt(key, decimals=4):
        if key in metrics and pd.notna(metrics[key]):
            try:
                return f"{float(metrics[key]):.{decimals}f}"
            except (TypeError, ValueError):
                return str(metrics[key])
        return "—"

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Accuracy",            fmt("accuracy"))
    c2.metric("Weighted Precision",  fmt("weightedPrecision"))
    c3.metric("Weighted Recall",     fmt("weightedRecall"))
    c4.metric("F1",                  fmt("f1"))

    c5, c6 = st.columns(2)
    c5.metric("Total rows",  fmt("total_rows", 0))
    c6.metric("Clean rows",  fmt("clean_rows", 0))


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    logger.info("Dashboard iniciado")
    st.set_page_config(
        page_title="Meteorisk Dashboard",
        layout="wide",
    )
    st.title("Meteorisk: Weather Risk Streaming Dashboard")
    st.caption(
        f"Ciudad: {config.CITY_NAME}  |  Tópico Kafka: {config.KAFKA_TOPIC}"
    )

    # Cargar datos
    processed_df   = load_parquet_folder(config.DATA_PROCESSED_PATH)
    aggregates_df  = load_parquet_folder(config.DATA_AGGREGATES_PATH)
    predictions_df = load_parquet_folder(config.DATA_PREDICTIONS_PATH)
    metrics_df     = load_model_metrics(MODEL_METRICS_FILE)

    # Sidebar y control de refresh
    auto_refresh, refresh_clicked = show_sidebar_status(
        processed_df, aggregates_df, predictions_df, metrics_df
    )

    # Cuerpo del dashboard
    show_alerts(processed_df, predictions_df)
    show_main_metrics(processed_df)
    st.markdown("---")
    show_weather_charts(processed_df)
    st.markdown("---")
    show_aggregate_charts(aggregates_df)
    st.markdown("---")
    show_predictions(predictions_df)
    st.markdown("---")
    show_model_metrics(metrics_df)

    # Refresco
    if refresh_clicked:
        st.rerun()
    if auto_refresh:
        time.sleep(AUTO_REFRESH_SECONDS)
        st.rerun()


if __name__ == "__main__":
    main()
