# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "24ffe816-b5e2-43d6-ac92-6c58a3704f60",
# META       "default_lakehouse_name": "lh_energia",
# META       "default_lakehouse_workspace_id": "211a9235-5f7e-459c-93c0-85e07e87f618",
# META       "known_lakehouses": [
# META         {
# META           "id": "24ffe816-b5e2-43d6-ac92-6c58a3704f60"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_bronze_ingesta_xm
# 
# Ingesta de la API de XM a la capa **Bronze** del Lakehouse `lh_energia`.
# 
# - **Idempotente:** cada petición se guarda en una ruta que depende solo de la métrica, el cruce y la ventana de fechas
#   (`Files/bronze/xm/{metrica}/{cruce}/{inicio}_{fin}.json`). Repetir una ventana sobrescribe el mismo archivo.
# - **Ventanas:** meses calendario (la API acepta máximo 31 días por petición en datos horarios y diarios).
# - **Incremental:** sin parámetros recarga los últimos 7 días (XM publica con 1 a 3 días de retraso y puede reliquidar).
# - **Backfill:** `start_date="2022-01-01"` y `end_date` vacío.
# - **Bitácora:** cada petición queda registrada en la tabla Delta `bronze_ingesta_log`.
# 
# Contrato de datos: `docs/data_contract.md`.

# PARAMETERS CELL ********************

# Parámetros (se pueden sobrescribir desde el pipeline de Data Factory)
start_date = ""        # "YYYY-MM-DD"; vacío = hoy - dias_incremental
end_date = ""          # "YYYY-MM-DD"; vacío = hoy
dias_incremental = 7
metricas = ""          # vacío = todas; o lista separada por comas, p. ej. "PrecBolsNaci,Gene"
incluir_listado_recursos = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import datetime as dt
import json
import time
import uuid

import requests

BASE_URL = "https://servapibi.xm.com.co"
BRONZE_ROOT = "Files/bronze/xm"
MAX_DIAS_POR_PETICION = 31

# MetricId, cruce (Entity), endpoint. Confirmado con scripts/explorar_xm.py el 2026-10-19.
METRICAS = [
    ("PrecBolsNaci", "Sistema", "hourly"),
    ("DemaCome", "Sistema", "hourly"),
    ("Gene", "Recurso", "hourly"),
    ("VoluUtilDiarEner", "Embalse", "daily"),
    ("AporEner", "Sistema", "daily"),
    ("PrecEsca", "Sistema", "daily"),
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def resolver_fechas(start_date: str, end_date: str, dias_incremental: int, hoy: dt.date | None = None) -> tuple[dt.date, dt.date]:
    """Convierte los parámetros en un rango de fechas válido."""
    hoy = hoy or dt.date.today()
    fin = dt.date.fromisoformat(end_date) if end_date else hoy
    inicio = dt.date.fromisoformat(start_date) if start_date else fin - dt.timedelta(days=dias_incremental)
    if inicio > fin:
        raise ValueError(f"start_date {inicio} es posterior a end_date {fin}")
    return inicio, fin


def ventanas_mensuales(inicio: dt.date, fin: dt.date) -> list[tuple[dt.date, dt.date]]:
    """Parte [inicio, fin] en ventanas de mes calendario (nunca más de 31 días)."""
    ventanas = []
    actual = inicio
    while actual <= fin:
        siguiente_mes = (actual.replace(day=1) + dt.timedelta(days=32)).replace(day=1)
        cierre = min(siguiente_mes - dt.timedelta(days=1), fin)
        ventanas.append((actual, cierre))
        actual = cierre + dt.timedelta(days=1)
    assert all((b - a).days + 1 <= MAX_DIAS_POR_PETICION for a, b in ventanas)
    return ventanas


def ruta_bronze(metrica: str, cruce: str, inicio: dt.date, fin: dt.date) -> str:
    return f"{BRONZE_ROOT}/{metrica}/{cruce}/{inicio:%Y-%m-%d}_{fin:%Y-%m-%d}.json"


def consultar_xm(endpoint: str, cuerpo: dict, intentos: int = 4, espera_base: float = 2.0) -> dict:
    """POST a la API de XM con reintentos y espera exponencial (2, 4, 8 s)."""
    for intento in range(1, intentos + 1):
        try:
            r = requests.post(f"{BASE_URL}/{endpoint}", json=cuerpo, timeout=120)
            if r.status_code == 200:
                return r.json()
            error = f"HTTP {r.status_code}: {r.text[:200]}"
        except requests.RequestException as exc:
            error = str(exc)
        if intento == intentos:
            raise RuntimeError(f"Falló {endpoint} {cuerpo.get('MetricId')} tras {intentos} intentos: {error}")
        time.sleep(espera_base * 2 ** (intento - 1))


def contar_registros(respuesta: dict) -> int:
    """Número de filas entidad-día que trae la respuesta."""
    total = 0
    for item in respuesta.get("Items", []):
        for clave, valor in item.items():
            if clave.endswith("Entities") and isinstance(valor, list):
                total += len(valor)
    return total

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def guardar_json(ruta: str, contenido: dict) -> None:
    """Escribe en OneLake (Lakehouse por defecto) sobrescribiendo si existe."""
    notebookutils.fs.put(ruta, json.dumps(contenido, ensure_ascii=False), True)


def ingerir_metrica(metrica: str, cruce: str, endpoint: str, inicio: dt.date, fin: dt.date, run_id: str) -> list[dict]:
    bitacora = []
    for v_inicio, v_fin in ventanas_mensuales(inicio, fin):
        cuerpo = {"MetricId": metrica, "Entity": cruce, "Filter": [],
                  "StartDate": f"{v_inicio:%Y-%m-%d}", "EndDate": f"{v_fin:%Y-%m-%d}"}
        ruta = ruta_bronze(metrica, cruce, v_inicio, v_fin)
        t0 = time.time()
        try:
            respuesta = consultar_xm(endpoint, cuerpo)
            sobre = {
                "_metadata": {"fuente": f"{BASE_URL}/{endpoint}", "peticion": cuerpo, "run_id": run_id,
                              "ingestado_utc": dt.datetime.utcnow().isoformat(timespec="seconds")},
                "respuesta": respuesta,
            }
            guardar_json(ruta, sobre)
            estado, filas, error = "ok", contar_registros(respuesta), None
        except Exception as exc:  # se registra y se sigue con la siguiente ventana
            estado, filas, error = "error", 0, str(exc)[:500]
        bitacora.append({"run_id": run_id, "metrica": metrica, "cruce": cruce,
                         "ventana_inicio": v_inicio, "ventana_fin": v_fin, "ruta": ruta,
                         "estado": estado, "registros": filas, "segundos": round(time.time() - t0, 2),
                         "error": error, "ingestado_utc": dt.datetime.utcnow()})
        print(f"{estado:5} {metrica:17} {v_inicio} → {v_fin}  {filas:6} registros")
    return bitacora


def ingerir_listado_recursos(run_id: str) -> dict:
    hoy = dt.date.today()
    respuesta = consultar_xm("lists", {"MetricId": "ListadoRecursos", "Entity": "Sistema"})
    ruta = f"{BRONZE_ROOT}/ListadoRecursos/Sistema/snapshot_{hoy:%Y-%m-%d}.json"
    guardar_json(ruta, {"_metadata": {"run_id": run_id, "ingestado_utc": dt.datetime.utcnow().isoformat(timespec="seconds")},
                        "respuesta": respuesta})
    filas = sum(len(i.get("ListEntities", [])) for i in respuesta.get("Items", []))
    print(f"ok    ListadoRecursos   snapshot {hoy}  {filas:6} recursos")
    return {"run_id": run_id, "metrica": "ListadoRecursos", "cruce": "Sistema", "ventana_inicio": hoy,
            "ventana_fin": hoy, "ruta": ruta, "estado": "ok", "registros": filas, "segundos": 0.0,
            "error": None, "ingestado_utc": dt.datetime.utcnow()}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

inicio, fin = resolver_fechas(start_date, end_date, dias_incremental)
seleccion = [m.strip() for m in metricas.split(",") if m.strip()]
a_cargar = [m for m in METRICAS if not seleccion or m[0] in seleccion]
run_id = str(uuid.uuid4())
print(f"run_id={run_id}  rango={inicio} → {fin}  métricas={[m[0] for m in a_cargar]}")

bitacora = []
for metrica, cruce, endpoint in a_cargar:
    bitacora += ingerir_metrica(metrica, cruce, endpoint, inicio, fin, run_id)
if incluir_listado_recursos:
    bitacora.append(ingerir_listado_recursos(run_id))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import types as T

esquema_log = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("metrica", T.StringType()),
    T.StructField("cruce", T.StringType()),
    T.StructField("ventana_inicio", T.DateType()),
    T.StructField("ventana_fin", T.DateType()),
    T.StructField("ruta", T.StringType()),
    T.StructField("estado", T.StringType()),
    T.StructField("registros", T.LongType()),
    T.StructField("segundos", T.DoubleType()),
    T.StructField("error", T.StringType()),
    T.StructField("ingestado_utc", T.TimestampType()),
])
spark.createDataFrame(bitacora, esquema_log).write.mode("append").saveAsTable("bronze_ingesta_log")

errores = [b for b in bitacora if b["estado"] != "ok"]
print(f"Ventanas: {len(bitacora)}  ok: {len(bitacora) - len(errores)}  errores: {len(errores)}")
if errores:
    # Falla el notebook para que el pipeline lo marque y avise; las ventanas ok ya quedaron guardadas.
    raise RuntimeError(f"{len(errores)} ventanas fallaron: " + "; ".join(f"{e['metrica']} {e['ventana_inicio']}" for e in errores[:10]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
