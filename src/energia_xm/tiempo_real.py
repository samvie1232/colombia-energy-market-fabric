"""Eventos de precio de bolsa para el Eventstream de Fabric (punto de conexión personalizado).

El Eventstream expone un endpoint compatible con Event Hubs. Se envía por HTTPS con un token SAS
para no depender de paquetes externos en el cuaderno.
"""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import hmac
import json
import time
import urllib.parse
from collections.abc import Callable, Iterable

import requests

DESFASE_COLOMBIA = dt.timedelta(hours=5)  # Colombia es UTC−5 todo el año
HORAS = [f"Hour{h:02d}" for h in range(1, 25)]


def parsear_cadena_conexion(cadena: str) -> dict[str, str]:
    """Separa Endpoint, SharedAccessKeyName, SharedAccessKey y EntityPath."""
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


def token_sas(uri: str, clave_nombre: str, clave: str, segundos: int = 3600, ahora: float | None = None) -> str:
    """Token SharedAccessSignature de Event Hubs para ``uri``."""
    expira = int((ahora if ahora is not None else time.time()) + segundos)
    recurso = urllib.parse.quote_plus(uri.lower())
    firma = hmac.new(clave.encode(), f"{recurso}\n{expira}".encode(), hashlib.sha256).digest()
    firma_url = urllib.parse.quote_plus(base64.b64encode(firma).decode())
    return f"SharedAccessSignature sr={recurso}&sig={firma_url}&se={expira}&skn={clave_nombre}"


def eventos_desde_xm(respuesta_precio: dict, escasez_por_fecha: dict[str, float], origen: str = "xm") -> list[dict]:
    """Convierte la respuesta horaria de PrecBolsNaci en un evento por hora (hora 0 = Hour01)."""
    eventos = []
    for item in respuesta_precio.get("Items", []):
        fecha = item["Date"][:10]
        for entidad in item.get("HourlyEntities", []):
            valores = entidad.get("Values", {})
            for hora, campo in enumerate(HORAS):
                texto = (valores.get(campo) or "").strip()
                if not texto:
                    continue
                eventos.append(crear_evento(fecha, hora, float(texto), escasez_por_fecha.get(fecha), origen))
    return sorted(eventos, key=lambda e: e["ts_utc"])


def crear_evento(fecha: str, hora: int, precio: float, escasez: float | None, origen: str) -> dict:
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


def escasez_desde_xm(respuesta_diaria: dict) -> dict[str, float]:
    """PrecEsca diario como {fecha: valor}."""
    resultado = {}
    for item in respuesta_diaria.get("Items", []):
        for entidad in item.get("DailyEntities", []):
            texto = (entidad.get("Value") or "").strip()
            if texto:
                resultado[item["Date"][:10]] = float(texto)
    return resultado


def solo_nuevos(eventos: Iterable[dict], ultimo_ts_utc: str | None) -> list[dict]:
    """Eventos posteriores al último enviado (evita alertas repetidas en Activator)."""
    return [e for e in eventos if ultimo_ts_utc is None or e["ts_utc"] > ultimo_ts_utc]


def enviar_eventos(
    eventos: list[dict],
    conexion: dict[str, str],
    pausa: float = 0.0,
    post: Callable[..., requests.Response] = requests.post,
    dormir: Callable[[float], None] = time.sleep,
) -> int:
    """Envía cada evento como un mensaje JSON. Devuelve cuántos se enviaron."""
    uri = f"https://{conexion['host']}/{conexion['entidad']}"
    url_envio = f"{uri}/messages?timeout=60&api-version=2014-01"
    token = token_sas(uri, conexion["clave_nombre"], conexion["clave"])
    cabeceras = {"Authorization": token, "Content-Type": "application/atom+xml;type=entry;charset=utf-8"}
    enviados = 0
    for evento in eventos:
        r = post(url_envio, headers=cabeceras, data=json.dumps(evento).encode(), timeout=60)
        if r.status_code != 201:
            raise RuntimeError(f"Eventstream respondió HTTP {r.status_code}: {r.text[:200]}")
        enviados += 1
        if pausa:
            dormir(pausa)
    return enviados
