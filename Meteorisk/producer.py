"""
producer.py
-----------
Productor de eventos meteorológicos para Meteorisk.

Flujo:
1. Consulta la API de Open-Meteo para Ciudad de México (config.py).
2. Convierte la respuesta horaria en una lista de observaciones.
3. Construye un evento JSON limpio por cada observación.
4. (Opcional, DEMO_MODE) Inyecta anomalías para que el modelo y el
   dashboard puedan mostrar alertas visibles.
5. Publica los eventos en el tópico Kafka definido en config.KAFKA_TOPIC.

Ejecución:
    python producer.py
"""

import json
import time
import sys

import requests
from kafka.errors import KafkaError, NoBrokersAvailable

import config
from utils.logging_config import get_logger
from utils.kafka_utils import create_kafka_producer, close_kafka_producer

logger = get_logger(__name__)


# ---------------------------------------------------------------------
# Constantes del módulo
# ---------------------------------------------------------------------
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "wind_gusts_10m",
    "surface_pressure",
    "apparent_temperature",
    "weather_code",
]

REQUEST_TIMEOUT_SECONDS = 10
SLEEP_BETWEEN_EVENTS_SECONDS = 0.5
SLEEP_BETWEEN_BATCHES_SECONDS = 30
DEMO_MODE = True


# ---------------------------------------------------------------------
# Open-Meteo
# ---------------------------------------------------------------------
def fetch_weather_data():
    """
    Consulta Open-Meteo y devuelve una lista de observaciones horarias.

    Cada observación es un dict con las claves de HOURLY_VARIABLES más
    `time` (timestamp ISO). Si la API responde vacío o falla la
    petición, regresa una lista vacía.
    """
    params = {
        "latitude": config.LATITUDE,
        "longitude": config.LONGITUDE,
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": "auto",
    }

    try:
        response = requests.get(
            OPEN_METEO_URL,
            params=params,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.error(f"Error al consultar Open-Meteo: {exc}")
        return []

    data = response.json()
    hourly = data.get("hourly")
    if not hourly or "time" not in hourly:
        logger.warning("Respuesta vacía o sin sección 'hourly' de Open-Meteo")
        return []

    timestamps = hourly["time"]

    # Transformamos los arrays paralelos en una lista de observaciones.
    observations = []
    for i, ts in enumerate(timestamps):
        obs = {"time": ts}
        for var in HOURLY_VARIABLES:
            values = hourly.get(var, [])
            obs[var] = values[i] if i < len(values) else None
        observations.append(obs)

    logger.debug(f"Open-Meteo API returned {len(observations)} observations")
    return observations


# ---------------------------------------------------------------------
# Construcción de eventos
# ---------------------------------------------------------------------
def build_event(observation):
    """
    Convierte una observación de Open-Meteo en un evento JSON limpio.

    Los valores None se conservan tal cual: Spark se encargará de la
    limpieza más adelante.
    """
    return {
        "city": config.CITY_NAME,
        "timestamp": observation.get("time"),
        "temperature": observation.get("temperature_2m"),
        "humidity": observation.get("relative_humidity_2m"),
        "precipitation": observation.get("precipitation"),
        "wind_speed": observation.get("wind_speed_10m"),
        "wind_gusts": observation.get("wind_gusts_10m"),
        "surface_pressure": observation.get("surface_pressure"),
        "apparent_temperature": observation.get("apparent_temperature"),
        "weather_code": observation.get("weather_code"),
    }


def apply_demo_anomalies(event, index):
    """
    Inyecta anomalías periódicas para tener casos moderados / críticos
    visibles en el dashboard y en el modelo. Solo se usa si DEMO_MODE.
    """
    # Cada 15 eventos: temperatura alta
    if index > 0 and index % 15 == 0:
        event["temperature"] = 36.5

    # Cada 25 eventos: precipitación muy alta
    if index > 0 and index % 25 == 0:
        event["precipitation"] = 55.0

    # Cada 40 eventos: viento extremo
    if index > 0 and index % 40 == 0:
        event["wind_speed"] = 70.0

    return event


# ---------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------
def send_events_to_kafka(producer, events):
    """
    Envía cada evento al tópico Kafka, imprime el evento y duerme un
    poco entre mensajes para simular un stream en tiempo real.
    """
    for event in events:
        try:
            producer.send(config.KAFKA_TOPIC, value=event)
            logger.debug(f"Evento enviado -> {event}")
        except KafkaError as exc:
            logger.error(f"Error al enviar evento a Kafka: {exc}")

        time.sleep(SLEEP_BETWEEN_EVENTS_SECONDS)

    producer.flush()


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    logger.info("Iniciando productor Meteorisk...")
    logger.info(f"  Broker Kafka : {config.KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"  Tópico       : {config.KAFKA_TOPIC}")
    logger.info(f"  Ciudad       : {config.CITY_NAME} ({config.LATITUDE}, {config.LONGITUDE})")
    logger.info(f"  DEMO_MODE    : {DEMO_MODE}")

    try:
        producer = create_kafka_producer(config.KAFKA_BOOTSTRAP_SERVERS)
    except NoBrokersAvailable:
        logger.critical(
            "No se pudo conectar a Kafka. "
            "¿Está levantado el contenedor con `docker compose up -d`?"
        )
        sys.exit(1)
    except KafkaError as exc:
        logger.critical(f"Error de Kafka al crear el productor: {exc}")
        sys.exit(1)

    event_counter = 0
    try:
        while True:
            observations = fetch_weather_data()
            if not observations:
                logger.warning("Sin observaciones, reintentando más tarde...")
                time.sleep(SLEEP_BETWEEN_BATCHES_SECONDS)
                continue

            events = []
            for obs in observations:
                event = build_event(obs)
                if DEMO_MODE:
                    event = apply_demo_anomalies(event, event_counter)
                events.append(event)
                event_counter += 1

            send_events_to_kafka(producer, events)

            logger.info(
                f"Lote enviado ({len(events)} eventos). "
                f"Durmiendo {SLEEP_BETWEEN_BATCHES_SECONDS}s..."
            )
            time.sleep(SLEEP_BETWEEN_BATCHES_SECONDS)

    except KeyboardInterrupt:
        logger.info("Interrupción recibida. Cerrando productor...")
    finally:
        close_kafka_producer(producer)


if __name__ == "__main__":
    main()
