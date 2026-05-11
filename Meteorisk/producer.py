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
import os
import random
import time
import sys
from datetime import datetime, timedelta

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
# Modo carga (LOAD_TEST_MODE)
# ---------------------------------------------------------------------
# Cuando LOAD_TEST_MODE=true, el productor ignora la API de Open-Meteo y
# genera eventos sintéticos a alta velocidad para validar que el pipeline
# soporta >=4096 eventos/segundo (requisito del enunciado, equivalente al
# casco de EEG: 16 sensores * 256 lecturas/s).
LOAD_TEST_MODE = os.getenv("LOAD_TEST_MODE", "false").lower() == "true"
LOAD_TEST_RATE = int(os.getenv("LOAD_TEST_RATE", "5000"))          # eventos/segundo
LOAD_TEST_DURATION = int(os.getenv("LOAD_TEST_DURATION", "60"))    # segundos
LOAD_TEST_ANOMALY_PROB = float(os.getenv("LOAD_TEST_ANOMALY_PROB", "0.05"))


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
# Modo carga: generación sintética a alta velocidad
# ---------------------------------------------------------------------
def synth_event(now):
    """
    Genera un evento sintético con distribuciones realistas para CDMX.
    Inyecta anomalías con probabilidad LOAD_TEST_ANOMALY_PROB para que
    el modelo tenga las tres clases representadas.
    """
    temperature = random.gauss(22.0, 4.0)
    humidity = max(0.0, min(100.0, random.gauss(60.0, 15.0)))
    precipitation = max(0.0, random.gauss(0.5, 1.5))
    wind_speed = max(0.0, random.gauss(12.0, 5.0))
    wind_gusts = wind_speed + max(0.0, random.gauss(5.0, 3.0))
    surface_pressure = random.gauss(1015.0, 4.0)
    apparent_temperature = temperature + random.gauss(0.0, 1.5)
    weather_code = random.choice([0, 1, 2, 3, 45, 61, 63, 80])

    if random.random() < LOAD_TEST_ANOMALY_PROB:
        kind = random.choice(["heat", "rain", "wind"])
        if kind == "heat":
            temperature = random.uniform(35.5, 42.0)
            apparent_temperature = temperature + 2.0
        elif kind == "rain":
            precipitation = random.uniform(50.5, 90.0)
            weather_code = 65
        else:
            wind_speed = random.uniform(60.5, 95.0)
            wind_gusts = wind_speed + 10.0

    return {
        "city": config.CITY_NAME,
        "timestamp": now.strftime("%Y-%m-%dT%H:%M"),
        "temperature": round(temperature, 2),
        "humidity": round(humidity, 2),
        "precipitation": round(precipitation, 2),
        "wind_speed": round(wind_speed, 2),
        "wind_gusts": round(wind_gusts, 2),
        "surface_pressure": round(surface_pressure, 2),
        "apparent_temperature": round(apparent_temperature, 2),
        "weather_code": int(weather_code),
    }


def run_load_test(producer):
    """
    Genera LOAD_TEST_RATE eventos/segundo durante LOAD_TEST_DURATION
    segundos. Reporta el throughput real al final.
    """
    logger.info(
        f"LOAD TEST: objetivo={LOAD_TEST_RATE} ev/s, duración={LOAD_TEST_DURATION}s, "
        f"total previsto={LOAD_TEST_RATE * LOAD_TEST_DURATION:,} eventos"
    )

    target_total = LOAD_TEST_RATE * LOAD_TEST_DURATION
    batch_size = max(50, LOAD_TEST_RATE // 100)  # 100 micro-batches por segundo
    sent = 0
    start = time.perf_counter()
    base_ts = datetime.now()

    next_tick = start
    while sent < target_total:
        for _ in range(batch_size):
            event_ts = base_ts + timedelta(milliseconds=sent * (1000 / LOAD_TEST_RATE))
            event = synth_event(event_ts)
            try:
                producer.send(config.KAFKA_TOPIC, value=event)
            except KafkaError as exc:
                logger.error(f"Kafka error: {exc}")
                break
            sent += 1
            if sent >= target_total:
                break

        elapsed = time.perf_counter() - start
        expected = sent / LOAD_TEST_RATE
        sleep = expected - elapsed
        if sleep > 0:
            time.sleep(sleep)

        # log de progreso cada ~5 segundos
        if int(elapsed) % 5 == 0 and elapsed - next_tick + start >= 5:
            rate = sent / max(elapsed, 1e-6)
            logger.info(f"  progreso: {sent:,}/{target_total:,} ev | rate={rate:,.0f} ev/s")
            next_tick = elapsed + start

    producer.flush()
    total_elapsed = time.perf_counter() - start
    real_rate = sent / max(total_elapsed, 1e-6)
    logger.info(
        f"LOAD TEST FINALIZADO: enviados={sent:,} en {total_elapsed:.2f}s "
        f"-> {real_rate:,.0f} ev/s (objetivo: {LOAD_TEST_RATE:,} ev/s)"
    )
    return {"sent": sent, "elapsed_s": total_elapsed, "rate_ev_s": real_rate}


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    logger.info("Iniciando productor Meteorisk...")
    logger.info(f"  Broker Kafka   : {config.KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"  Tópico         : {config.KAFKA_TOPIC}")
    logger.info(f"  Ciudad         : {config.CITY_NAME} ({config.LATITUDE}, {config.LONGITUDE})")
    logger.info(f"  DEMO_MODE      : {DEMO_MODE}")
    logger.info(f"  LOAD_TEST_MODE : {LOAD_TEST_MODE}")

    try:
        producer = create_kafka_producer(
            config.KAFKA_BOOTSTRAP_SERVERS,
            high_throughput=LOAD_TEST_MODE,
        )
    except NoBrokersAvailable:
        logger.critical(
            "No se pudo conectar a Kafka. "
            "¿Está levantado el contenedor con `docker compose up -d`?"
        )
        sys.exit(1)
    except KafkaError as exc:
        logger.critical(f"Error de Kafka al crear el productor: {exc}")
        sys.exit(1)

    # LOAD TEST: ejecutar una sola vez y salir
    if LOAD_TEST_MODE:
        try:
            stats = run_load_test(producer)
        except KeyboardInterrupt:
            logger.info("Load test interrumpido por el usuario.")
            stats = None
        finally:
            close_kafka_producer(producer)
        if stats:
            # Persistir resultado para el informe
            os.makedirs(config.DATA_METRICS_PATH, exist_ok=True)
            out = os.path.join(config.DATA_METRICS_PATH, "load_test_result.csv")
            with open(out, "w", encoding="utf-8") as f:
                f.write("metric,value\n")
                for k, v in stats.items():
                    f.write(f"{k},{v}\n")
            logger.info(f"Resultado del load test guardado en {out}")
        return

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
