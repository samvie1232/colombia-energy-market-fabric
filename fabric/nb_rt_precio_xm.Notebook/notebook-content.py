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

# # nb_rt_precio_xm: precio de bolsa al Eventstream
# 
# Envía el precio de bolsa horario como eventos JSON al Eventstream `es_precio_bolsa`
# (origen *punto de conexión personalizado*). De ahí van a Eventhouse `eh_energia` y Activator
# dispara el correo cuando el precio supera el umbral.
# 
# - **modo `xm`** (programado): consulta en XM los últimos `dias` de `PrecBolsNaci` y `PrecEsca`
#   y envía solo las horas posteriores a la última enviada (estado en `Files/rt/ultimo_enviado.txt`).
# - **modo `replay`** (demo): reenvía las 24 horas de un día de Silver con una pausa entre eventos.
#   Sin `fecha_replay` usa el día con el precio horario más alto, para ver la alerta disparar.
# 
# La cadena de conexión del Eventstream es un secreto: se guarda en `Files/secretos/es_precio_bolsa.txt`
# del Lakehouse `lh_energia` (los archivos del Lakehouse no van a Git), nunca en el cuaderno.
# 
# XM publica el precio con 1 a 3 días de retraso, así que esto es "casi tiempo real":
# el evento llega cuando XM publica, no a la hora del despacho.


# PARAMETERS CELL ********************

modo = "xm"              # "xm" o "replay"
dias = 7                 # modo xm: días hacia atrás a consultar
fecha_replay = ""        # modo replay: "YYYY-MM-DD"; vacío = día con el precio máximo
pausa_seg = 2.0          # modo replay: segundos entre eventos
ruta_secreto = "/lakehouse/default/Files/secretos/es_precio_bolsa.txt"
ruta_estado = "/lakehouse/default/Files/rt/ultimo_enviado.txt"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import base64
import datetime as dt
import hashlib
import hmac
import json
import os
import time
import urllib.parse

import requests

BASE_URL = "https://servapibi.xm.com.co"
DESFASE_COLOMBIA = dt.timedelta(hours=5)  # Colombia es UTC−5 todo el año
HORAS = [f"Hour{h:02d}" for h in range(1, 25)]

# Mismas funciones que src/energia_xm/tiempo_real.py (probadas en CI).


def consultar_xm(endpoint: str, cuerpo: dict, intentos: int = 4, espera_base: float = 2.0) -> dict:
    error = ""
    for intento in range(1, intentos + 1):
        try:
            r = requests.post(f"{BASE_URL}/{endpoint}", json=cuerpo, timeout=120)
            if r.status_code == 200:
                return r.json()
            error = f"HTTP {r.status_code}: {r.text[:200]}"
        except requests.RequestException as exc:
            error = str(exc)
        if intento < intentos:
            time.sleep(espera_base * 2 ** (intento - 1))
    raise RuntimeError(f"Falló {endpoint} {cuerpo.get('MetricId')} tras {intentos} intentos: {error}")


def parsear_cadena_conexion(cadena: str) -> dict:
    partes = {}
    for trozo in cadena.strip().split(";"):
        if "=" in trozo:
            clave, _, valor = trozo.partition("=")
            partes[clave.strip()] = valor.strip()
    faltan = {"Endpoint", "SharedAccessKeyName", "SharedAccessKey", "EntityPath"} - partes.keys()
    if faltan:
        raise ValueError(f"A la cadena de conexión le falta: {', '.join(sorted(faltan))}")
    host = partes["Endpoint"].removeprefix("sb://").rstrip("/")
    return {"host": host, "entidad": partes["EntityPath"],
            "clave_nombre": partes["SharedAccessKeyName"], "clave": partes["SharedAccessKey"]}


def token_sas(uri: str, clave_nombre: str, clave: str, segundos: int = 3600) -> str:
    expira = int(time.time() + segundos)
    recurso = urllib.parse.quote_plus(uri.lower())
    firma = hmac.new(clave.encode(), f"{recurso}\n{expira}".encode(), hashlib.sha256).digest()
    firma_url = urllib.parse.quote_plus(base64.b64encode(firma).decode())
    return f"SharedAccessSignature sr={recurso}&sig={firma_url}&se={expira}&skn={clave_nombre}"


def crear_evento(fecha: str, hora: int, precio: float, escasez, origen: str) -> dict:
    ts_colombia = dt.datetime.fromisoformat(fecha) + dt.timedelta(hours=hora)
    return {
        "fecha": fecha,
        "hora": hora,
        "ts_colombia": ts_colombia.strftime("%Y-%m-%dT%H:%M:%S"),
        "ts_utc": (ts_colombia + DESFASE_COLOMBIA).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "precio_bolsa_cop_kwh": round(precio, 4),
        "precio_escasez_cop_kwh": None if escasez is None else round(escasez, 4),
        "sobre_escasez": None if escasez is None else precio > escasez,
        "origen": origen,
    }


def eventos_desde_xm(respuesta_precio: dict, escasez_por_fecha: dict, origen: str = "xm") -> list:
    eventos = []
    for item in respuesta_precio.get("Items", []):
        fecha = item["Date"][:10]
        for entidad in item.get("HourlyEntities", []):
            valores = entidad.get("Values", {})
            for hora, campo in enumerate(HORAS):
                texto = (valores.get(campo) or "").strip()
                if texto:
                    eventos.append(crear_evento(fecha, hora, float(texto), escasez_por_fecha.get(fecha), origen))
    return sorted(eventos, key=lambda e: e["ts_utc"])


def escasez_desde_xm(respuesta_diaria: dict) -> dict:
    resultado = {}
    for item in respuesta_diaria.get("Items", []):
        for entidad in item.get("DailyEntities", []):
            texto = (entidad.get("Value") or "").strip()
            if texto:
                resultado[item["Date"][:10]] = float(texto)
    return resultado


def solo_nuevos(eventos: list, ultimo_ts_utc) -> list:
    return [e for e in eventos if ultimo_ts_utc is None or e["ts_utc"] > ultimo_ts_utc]


def enviar_eventos(eventos: list, conexion: dict, pausa: float = 0.0) -> int:
    uri = f"https://{conexion['host']}/{conexion['entidad']}"
    url_envio = f"{uri}/messages?timeout=60&api-version=2014-01"
    cabeceras = {"Authorization": token_sas(uri, conexion["clave_nombre"], conexion["clave"]),
                 "Content-Type": "application/atom+xml;type=entry;charset=utf-8"}
    enviados = 0
    for evento in eventos:
        r = requests.post(url_envio, headers=cabeceras, data=json.dumps(evento).encode(), timeout=60)
        if r.status_code != 201:
            raise RuntimeError(f"Eventstream respondió HTTP {r.status_code}: {r.text[:200]}")
        enviados += 1
        if pausa:
            time.sleep(pausa)
    return enviados

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not os.path.exists(ruta_secreto):
    raise FileNotFoundError(
        f"No existe {ruta_secreto}. Sube un archivo de texto con la cadena de conexión del Eventstream "
        "a Files/secretos/ del Lakehouse lh_energia."
    )
with open(ruta_secreto, encoding="utf-8") as f:
    conexion = parsear_cadena_conexion(f.read())

if modo == "xm":
    fin = dt.date.today()
    inicio = fin - dt.timedelta(days=dias)
    rango = {"Entity": "Sistema", "Filter": [], "StartDate": f"{inicio:%Y-%m-%d}", "EndDate": f"{fin:%Y-%m-%d}"}
    precio = consultar_xm("hourly", {"MetricId": "PrecBolsNaci", **rango})
    escasez = escasez_desde_xm(consultar_xm("daily", {"MetricId": "PrecEsca", **rango}))
    eventos = eventos_desde_xm(precio, escasez)

    ultimo = None
    if os.path.exists(ruta_estado):
        with open(ruta_estado, encoding="utf-8") as f:
            ultimo = f.read().strip() or None
    nuevos = solo_nuevos(eventos, ultimo)
    enviados = enviar_eventos(nuevos, conexion)
    if nuevos:
        os.makedirs(os.path.dirname(ruta_estado), exist_ok=True)
        with open(ruta_estado, "w", encoding="utf-8") as f:
            f.write(nuevos[-1]["ts_utc"])
    print(f"XM {inicio}→{fin}: {len(eventos)} horas publicadas, {enviados} nuevas enviadas (última previa: {ultimo})")

elif modo == "replay":
    if not fecha_replay:
        fecha_replay = str(spark.sql(
            "SELECT fecha FROM silver.precio_bolsa_hora ORDER BY precio_cop_kwh DESC NULLS LAST LIMIT 1"
        ).first()["fecha"])
    filas = spark.sql(f"""
        SELECT p.hora, CAST(p.precio_cop_kwh AS double) AS precio, CAST(e.precio_escasez_cop_kwh AS double) AS escasez
        FROM silver.precio_bolsa_hora p
        LEFT JOIN silver.precio_escasez_dia e ON e.fecha = p.fecha
        WHERE p.fecha = DATE'{fecha_replay}' AND p.precio_cop_kwh IS NOT NULL
        ORDER BY p.hora
    """).collect()
    eventos = [crear_evento(fecha_replay, f["hora"], f["precio"], f["escasez"], "replay") for f in filas]
    enviados = enviar_eventos(eventos, conexion, pausa=pausa_seg)
    maximo = max((e["precio_bolsa_cop_kwh"] for e in eventos), default=None)
    print(f"Replay {fecha_replay}: {enviados} eventos enviados, precio máximo {maximo} COP/kWh")

else:
    raise ValueError(f"modo debe ser 'xm' o 'replay', no {modo!r}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
