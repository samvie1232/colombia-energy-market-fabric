"""Cliente mínimo de la API de XM (https://servapibi.xm.com.co)."""

from __future__ import annotations

import time
from collections.abc import Callable

import requests

BASE_URL = "https://servapibi.xm.com.co"

# MetricId, cruce (Entity), endpoint. Confirmado con scripts/explorar_xm.py el 2026-10-19.
METRICAS = [
    ("PrecBolsNaci", "Sistema", "hourly"),
    ("DemaCome", "Sistema", "hourly"),
    ("Gene", "Recurso", "hourly"),
    ("VoluUtilDiarEner", "Embalse", "daily"),
    ("AporEner", "Sistema", "daily"),
    ("PrecEsca", "Sistema", "daily"),
]


def consultar_xm(
    endpoint: str,
    cuerpo: dict,
    intentos: int = 4,
    espera_base: float = 2.0,
    post: Callable[..., requests.Response] = requests.post,
    dormir: Callable[[float], None] = time.sleep,
) -> dict:
    """POST a la API con reintentos y espera exponencial (2, 4, 8 s).

    ``post`` y ``dormir`` se pueden reemplazar en las pruebas.
    """
    error = ""
    for intento in range(1, intentos + 1):
        try:
            r = post(f"{BASE_URL}/{endpoint}", json=cuerpo, timeout=120)
            if r.status_code == 200:
                return r.json()
            error = f"HTTP {r.status_code}: {r.text[:200]}"
        except requests.RequestException as exc:
            error = str(exc)
        if intento < intentos:
            dormir(espera_base * 2 ** (intento - 1))
    raise RuntimeError(f"Falló {endpoint} {cuerpo.get('MetricId')} tras {intentos} intentos: {error}")


def contar_registros(respuesta: dict) -> int:
    """Número de filas entidad-día que trae una respuesta (HourlyEntities, DailyEntities, ...)."""
    total = 0
    for item in respuesta.get("Items", []):
        for clave, valor in item.items():
            if clave.endswith("Entities") and isinstance(valor, list):
                total += len(valor)
    return total
