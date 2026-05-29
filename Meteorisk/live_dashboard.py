
"""
live_dashboard.py
-----------------
Dashboard en vivo de Meteorisk.

Consume directamente del tópico Kafka `weather_stream` con kafka-python,
clasifica cada evento usando las reglas deterministas del modelo
(utils.risk_rules) y muestra:

  - Indicador LIVE con tiempo desde el último evento.
  - KPIs del último evento + predicción de riesgo (con color).
  - Contador acumulado por categoría (normal / moderate / critical).
  - Mini-gráficas de temperatura y predicción a lo largo del tiempo.
  - Tabla de los últimos eventos con su clasificación.

Ejecución:
    streamlit run live_dashboard.py
"""

import json
import time
from collections import Counter, deque
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

import config
from utils.logging_config import setup_logging
from utils.risk_rules import classify_event

logger = setup_logging(__name__)


# ---------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------
BUFFER_SIZE = 200
AUTO_REFRESH_SECONDS = 2
CHART_WINDOW = 100
TABLE_ROWS = 15

RISK_COLOR = {
    "normal": "#22c55e",     # verde
    "moderate": "#f59e0b",   # naranja
    "critical": "#ef4444",   # rojo
    "unknown": "#9ca3af",    # gris
}


# ---------------------------------------------------------------------
# Kafka consumer (cacheado para reutilizarse entre reruns)
# ---------------------------------------------------------------------
@st.cache_resource(show_spinner="Conectando a Kafka...")
def get_kafka_consumer():
    """
    Crea un KafkaConsumer único para esta sesión del dashboard.
    `auto_offset_reset='latest'` para que solo veamos eventos nuevos
    a partir del momento en que se abre el dashboard.
    """
    try:
        consumer = KafkaConsumer(
            config.KAFKA_TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id=f"live-dashboard-{int(time.time())}",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=500,
        )
        logger.info(
            f"Consumer creado | broker={config.KAFKA_BOOTSTRAP_SERVERS} "
            f"topic={config.KAFKA_TOPIC}"
        )
        return consumer
    except NoBrokersAvailable as exc:
        logger.error(f"NoBrokersAvailable: {exc}")
        return None
    except KafkaError as exc:
        logger.error(f"KafkaError al crear consumer: {exc}")
        return None


# ---------------------------------------------------------------------
# Polling
# ---------------------------------------------------------------------
def poll_new_events(consumer, buffer: deque) -> int:
    """
    Hace poll a Kafka y agrega los nuevos eventos al buffer enriquecidos
    con `risk_prediction` y `_received_at`.
    Devuelve el número de eventos nuevos.
    """
    new_count = 0
    try:
        records = consumer.poll(timeout_ms=500, max_records=200)
    except Exception as exc:
        logger.error(f"Error en poll(): {exc}")
        return 0

    for _topic_partition, messages in records.items():
        for msg in messages:
            event = msg.value
            event["risk_prediction"] = classify_event(event)
            event["_received_at"] = datetime.now()
            buffer.append(event)
            new_count += 1
    return new_count


# ---------------------------------------------------------------------
# Render helpers
# ---------------------------------------------------------------------
def render_header(last_event_at):
    st.title("Meteorisk · Live Stream")

    if last_event_at is None:
        delta_txt = "sin eventos aún"
    else:
        delta = (datetime.now() - last_event_at).total_seconds()
        delta_txt = f"hace {delta:.1f} s"

    st.caption(
        f"🔴 LIVE · Tópico: `{config.KAFKA_TOPIC}` · "
        f"Broker: `{config.KAFKA_BOOTSTRAP_SERVERS}` · "
        f"Ciudad: {config.CITY_NAME} · "
        f"Última actualización: {delta_txt}"
    )


def render_alert_banner(last_event):
    if last_event is None:
        st.info("Esperando primer evento... Ejecuta `python producer.py` en otra terminal.")
        return

    risk = last_event.get("risk_prediction", "unknown")
    if risk == "critical":
        st.error("⚠️ Riesgo CRÍTICO detectado en el último evento")
    elif risk == "moderate":
        st.warning("⚡ Riesgo moderado en el último evento")
    elif risk == "normal":
        st.success("✅ Condiciones normales")
    else:
        st.info("Predicción no disponible para el último evento")


def render_last_event_kpis(last_event):
    st.subheader("Último evento recibido")
    if last_event is None:
        st.info("Aún no hay datos.")
        return

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Temperatura (°C)", f"{last_event.get('temperature', 0):.1f}")
    c2.metric("Humedad (%)", f"{last_event.get('humidity', 0):.1f}")
    c3.metric("Precipitación (mm)", f"{last_event.get('precipitation', 0):.1f}")
    c4.metric("Viento (km/h)", f"{last_event.get('wind_speed', 0):.1f}")

    risk = last_event.get("risk_prediction", "unknown")
    color = RISK_COLOR.get(risk, "#9ca3af")
    c5.markdown(
        f"""
        <div style="text-align:center">
          <div style="font-size:0.875rem;color:#6b7280;margin-bottom:4px">Predicción</div>
          <div style="
            display:inline-block;
            padding:8px 16px;
            background:{color};
            color:white;
            border-radius:8px;
            font-weight:700;
            font-size:1.25rem;
            text-transform:uppercase;
          ">{risk}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_risk_counters(buffer):
    st.subheader("Distribución acumulada (sesión actual)")
    counts = Counter(ev.get("risk_prediction", "unknown") for ev in buffer)
    c1, c2, c3 = st.columns(3)
    c1.metric("🟢 Normal", counts.get("normal", 0))
    c2.metric("🟠 Moderate", counts.get("moderate", 0))
    c3.metric("🔴 Critical", counts.get("critical", 0))


def render_live_charts(buffer):
    st.subheader(f"Histórico reciente (últimos {CHART_WINDOW} eventos)")
    if not buffer:
        st.info("Sin datos para graficar.")
        return

    recent = list(buffer)[-CHART_WINDOW:]
    df = pd.DataFrame(recent)

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.sort_values("timestamp")
    else:
        df["timestamp"] = df["_received_at"]

    c1, c2 = st.columns(2)
    with c1:
        fig = px.line(
            df,
            x="timestamp",
            y="temperature",
            title="Temperatura (°C)",
            markers=True,
        )
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        fig = px.scatter(
            df,
            x="timestamp",
            y="temperature",
            color="risk_prediction",
            color_discrete_map=RISK_COLOR,
            title="Predicción a lo largo del tiempo",
            category_orders={"risk_prediction": ["normal", "moderate", "critical", "unknown"]},
        )
        st.plotly_chart(fig, use_container_width=True)


def render_recent_table(buffer):
    st.subheader(f"Últimos {TABLE_ROWS} eventos")
    if not buffer:
        st.info("Sin datos.")
        return

    cols = ["timestamp", "temperature", "humidity", "precipitation", "wind_speed", "risk_prediction"]
    rows = list(buffer)[-TABLE_ROWS:][::-1]
    df = pd.DataFrame(rows)
    present = [c for c in cols if c in df.columns]

    def color_risk(val):
        color = RISK_COLOR.get(val, "#9ca3af")
        return f"background-color: {color}; color: white; font-weight: 600;"

    styled = df[present].style.map(color_risk, subset=["risk_prediction"])
    st.dataframe(styled, use_container_width=True, hide_index=True)


def render_sidebar(consumer_ok, buffer, total_received, auto_refresh_default):
    st.sidebar.header("Estado")
    if consumer_ok:
        st.sidebar.success(f"✅ Conectado a Kafka\n`{config.KAFKA_BOOTSTRAP_SERVERS}`")
    else:
        st.sidebar.error("❌ No conectado a Kafka")

    st.sidebar.metric("Eventos recibidos (sesión)", total_received)
    st.sidebar.metric("Buffer", f"{len(buffer)} / {BUFFER_SIZE}")

    st.sidebar.markdown("---")
    auto_refresh = st.sidebar.checkbox(
        f"Auto-refresh cada {AUTO_REFRESH_SECONDS}s",
        value=auto_refresh_default,
    )
    refresh_now = st.sidebar.button("🔄 Actualizar ahora")
    clear_buffer = st.sidebar.button("🗑 Limpiar buffer")

    st.sidebar.markdown("---")
    st.sidebar.caption(
        "Este dashboard consume Kafka directamente y aplica las reglas "
        "deterministas de `utils.risk_rules`, equivalentes al RandomForest "
        "entrenado en `train_model.py`."
    )

    return auto_refresh, refresh_now, clear_buffer


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    st.set_page_config(page_title="Meteorisk · Live", layout="wide")

    # Estado de sesión
    if "buffer" not in st.session_state:
        st.session_state.buffer = deque(maxlen=BUFFER_SIZE)
    if "total_received" not in st.session_state:
        st.session_state.total_received = 0
    if "last_event_at" not in st.session_state:
        st.session_state.last_event_at = None

    # Consumer
    consumer = get_kafka_consumer()
    consumer_ok = consumer is not None

    if not consumer_ok:
        st.title("Meteorisk · Live Stream")
        st.error(
            "No se pudo conectar a Kafka en "
            f"`{config.KAFKA_BOOTSTRAP_SERVERS}`. "
            "¿Está levantado el contenedor con `docker compose up -d`?"
        )
        if st.button("Reintentar conexión"):
            get_kafka_consumer.clear()
            st.rerun()
        st.stop()

    # Poll
    new_count = poll_new_events(consumer, st.session_state.buffer)
    if new_count > 0:
        st.session_state.total_received += new_count
        st.session_state.last_event_at = datetime.now()

    # Sidebar
    auto_refresh, refresh_now, clear_buffer = render_sidebar(
        consumer_ok,
        st.session_state.buffer,
        st.session_state.total_received,
        auto_refresh_default=True,
    )

    if clear_buffer:
        st.session_state.buffer.clear()
        st.session_state.total_received = 0
        st.session_state.last_event_at = None
        st.rerun()

    # Cuerpo
    last_event = st.session_state.buffer[-1] if st.session_state.buffer else None

    render_header(st.session_state.last_event_at)
    render_alert_banner(last_event)
    render_last_event_kpis(last_event)
    st.markdown("---")
    render_risk_counters(st.session_state.buffer)
    st.markdown("---")
    render_live_charts(st.session_state.buffer)
    st.markdown("---")
    render_recent_table(st.session_state.buffer)

    # Refresh
    if refresh_now:
        st.rerun()
    if auto_refresh:
        time.sleep(AUTO_REFRESH_SECONDS)
        st.rerun()


if __name__ == "__main__":
    main()
