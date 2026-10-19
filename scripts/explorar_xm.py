"""Explora la API de XM y confirma las métricas del contrato de datos.

Uso (en WSL):
    uv run --with requests --with pandas python scripts/explorar_xm.py

Escribe docs/xm_metricas.csv (inventario completo) y imprime, para cada métrica
candidata, sus cruces (Entity), granularidad (Type), y una muestra de 2 días.
"""
import datetime as dt
from pathlib import Path

import pandas as pd
import requests

BASE = "https://servapibi.xm.com.co"
ENDPOINTS = {"HourlyEntities": "hourly", "DailyEntities": "daily",
             "MonthlyEntities": "monthly", "AnnualEntities": "annual"}

# Candidatas para el MVP; el script confirma si existen y con qué cruce.
CANDIDATAS = [
    ("PrecBolsNaci", "Sistema"),       # precio de bolsa nacional
    ("DemaCome", "Sistema"),           # demanda comercial
    ("Gene", "Recurso"),               # generación real por recurso
    ("VoluUtilDiarEner", "Embalse"),   # volumen útil diario por embalse
    ("AporEner", "Sistema"),           # aportes hídricos
    ("PrecEsca", "Sistema"),           # precio de escasez
]


def inventario() -> pd.DataFrame:
    r = requests.post(f"{BASE}/lists", json={"MetricId": "ListadoMetricas"}, timeout=60)
    r.raise_for_status()
    df = pd.json_normalize(r.json()["Items"], "ListEntities", "Date", sep="_")
    df.columns = [c.replace("Values_", "") for c in df.columns]
    return df


def muestra(metric: str, entity: str, tipo: str) -> pd.DataFrame:
    end = dt.date.today() - dt.timedelta(days=3)
    body = {"MetricId": metric, "Entity": entity, "Filter": [],
            "StartDate": str(end - dt.timedelta(days=1)), "EndDate": str(end)}
    r = requests.post(f"{BASE}/{ENDPOINTS[tipo]}", json=body, timeout=60)
    r.raise_for_status()
    return pd.json_normalize(r.json()["Items"], tipo, "Date", sep="_")


def ultima_fecha(metric: str, entity: str, tipo: str) -> str:
    """Retraso de publicación: última fecha con datos en los últimos 10 días."""
    end = dt.date.today()
    body = {"MetricId": metric, "Entity": entity, "Filter": [],
            "StartDate": str(end - dt.timedelta(days=10)), "EndDate": str(end)}
    r = requests.post(f"{BASE}/{ENDPOINTS[tipo]}", json=body, timeout=60)
    fechas = [i["Date"] for i in r.json().get("Items", [])]
    return max(fechas) if fechas else "sin datos"


def main() -> None:
    inv = inventario()
    Path("docs").mkdir(exist_ok=True)
    inv.to_csv("docs/xm_metricas.csv", index=False)
    print(f"Inventario: {len(inv)} combinaciones métrica/cruce -> docs/xm_metricas.csv\n")

    for metric, entity in CANDIDATAS:
        fila = inv.query("MetricId == @metric")
        if fila.empty:
            print(f"!! {metric} no existe. Busca alternativas en docs/xm_metricas.csv\n")
            continue
        cols = [c for c in ["MetricId", "MetricName", "Entity", "Type", "MaxDays", "MetricUnits"] if c in fila]
        print(fila[cols].to_string(index=False))
        sel = fila.query("Entity == @entity")
        if sel.empty or sel.Type.iloc[0] not in ENDPOINTS:
            print(f"   (cruce {entity} no disponible o no temporal)\n")
            continue
        tipo = sel.Type.iloc[0]
        df = muestra(metric, entity, tipo)
        print(f"   muestra {entity}: {df.shape[0]} filas, columnas {list(df.columns)[:8]}...")
        print(df.head(3).to_string(index=False))
        print(f"   última fecha publicada: {ultima_fecha(metric, entity, tipo)}\n")


if __name__ == "__main__":
    main()
