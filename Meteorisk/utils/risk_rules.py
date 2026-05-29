"""
utils/risk_rules.py
-------------------
Reglas deterministas de clasificación de riesgo meteorológico, equivalentes
a las usadas en train_model.add_risk_labels.

Se utilizan en live_dashboard.py para clasificar eventos en tiempo real sin
tener que cargar Spark MLlib en el proceso del dashboard. Las reglas son
idénticas a las que aprendió el RandomForest.

Prioridad: critical > moderate > normal.
"""

RISK_CRITICAL = "critical"
RISK_MODERATE = "moderate"
RISK_NORMAL = "normal"
RISK_UNKNOWN = "unknown"


def classify_risk(temperature, wind_speed, precipitation) -> str:
    """
    Clasifica un evento meteorológico en {critical, moderate, normal, unknown}.

    Reglas (idénticas a train_model.add_risk_labels):
      - critical: temperature > 35  o  wind_speed > 60  o  precipitation > 50
      - moderate: 30 <= temperature <= 35  o  20 <= precipitation <= 50
                  o  40 <= wind_speed <= 60
      - normal: cualquier otro caso
      - unknown: alguna de las tres variables es None
    """
    if temperature is None or wind_speed is None or precipitation is None:
        return RISK_UNKNOWN

    if temperature > 35 or wind_speed > 60 or precipitation > 50:
        return RISK_CRITICAL

    if (
        (30 <= temperature <= 35)
        or (20 <= precipitation <= 50)
        or (40 <= wind_speed <= 60)
    ):
        return RISK_MODERATE

    return RISK_NORMAL


def classify_event(event: dict) -> str:
    """Atajo: clasifica un evento JSON tal como lo emite producer.py."""
    return classify_risk(
        event.get("temperature"),
        event.get("wind_speed"),
        event.get("precipitation"),
    )
