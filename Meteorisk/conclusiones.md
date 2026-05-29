# Conclusiones — Meteorisk

## Objetivo del proyecto

Meteorisk implementa un pipeline end-to-end de procesamiento de datos meteorológicos en streaming para la Ciudad de México: ingesta desde la API pública Open-Meteo, transporte mediante Apache Kafka, procesamiento distribuido con Spark Structured Streaming, clasificación de riesgo con un modelo RandomForest (Spark MLlib) y visualización interactiva en dos dashboards Streamlit (uno histórico y uno en vivo).

## Logros obtenidos

- **Ingesta en tiempo real** (`producer.py`): consumo periódico de Open-Meteo y publicación al tópico `weather_stream` de Kafka. Incluye `DEMO_MODE` (inyección de anomalías cada 15/25/40 eventos para garantizar las tres clases de riesgo en demos) y `LOAD_TEST_MODE` (generación sintética validada a >4 000 ev/s).
- **Streaming distribuido** (`streaming.py`): limpieza, deduplicación, agregación por ventanas de 1 minuto y persistencia incremental en Parquet con *checkpoints* de Spark para garantizar idempotencia.
- **Modelo de machine learning** (`train_model.py`): `RandomForestClassifier` (numTrees=30, maxDepth=5) que clasifica eventos en `normal` / `moderate` / `critical` a partir de 8 features (temperatura, humedad, precipitación, viento, ráfagas, presión, sensación térmica y código meteorológico). Métricas persistidas en `data/metrics/model_metrics.csv` (accuracy, weightedPrecision, weightedRecall, F1).
- **Predicción en streaming** (`predict_stream.py`): MLlib aplicado sobre el stream con doble sink — consola y Parquet en `data/predictions/`.
- **Doble dashboard**:
  - `dashboard.py` — vista analítica/histórica sobre los Parquet (series temporales, agregados por ventana, distribución de predicciones y métricas del modelo).
  - `live_dashboard.py` — vista en vivo que consume Kafka directamente con `kafka-python`, clasifica cada evento mediante `utils/risk_rules.py` y refresca cada 2 segundos, mostrando KPIs, contadores acumulados, mini-gráficas y la tabla de los últimos 15 eventos con color por nivel de riesgo.
- **API REST** (`http_api_server.py`): endpoints Flask para integración con herramientas BI externas (Power BI, Tableau).
- **Esquema centralizado** (`utils/schema_registry.py`) y configuración única (`config.py`) que evitan inconsistencias entre módulos.

## Limitaciones reconocidas

- El `live_dashboard.py` aplica las reglas deterministas equivalentes al modelo en Python puro para evitar arrancar Spark en el proceso del dashboard. Esto reproduce la decisión del RandomForest en la inmensa mayoría de los casos pero no expone la probabilidad por clase.
- No existe orquestación unificada: cada componente del pipeline se arranca manualmente en una terminal distinta.
- No hay alerting externo (Slack, correo) cuando se mantiene un estado `critical` sostenido.
- El modelo se entrena solo sobre una ciudad (Ciudad de México); la generalización a otras geografías requeriría re-entrenamiento.

## Extensiones futuras

- **Servicio de inferencia real**: exponer el modelo MLlib detrás de un microservicio (MLflow Serve o FastAPI + Spark Connect) y consumirlo desde `live_dashboard.py` para mostrar probabilidades por clase, no solo la etiqueta.
- **MLOps**: versionado y tracking de experimentos con MLflow, re-entrenamiento programado y comparación A/B entre versiones del modelo.
- **Alerting**: notificaciones automáticas (Slack/email/webhook) cuando se detecten N eventos `critical` consecutivos.
- **Despliegue contenedorizado**: un `docker-compose.yml` unificado que levante Kafka, el productor, los jobs de Spark y los dashboards.
- **Multi-ciudad**: extender el productor para emitir eventos de varias ciudades particionando por `city` en Kafka, y adaptar los dashboards para filtrar por ciudad.
- **Persistencia en serie temporal**: integrar TimescaleDB o InfluxDB como destino adicional para consultas analíticas más eficientes que los Parquet apilados.
