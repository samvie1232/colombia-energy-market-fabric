import base64
import hashlib
import hmac
import json
import urllib.parse

import pytest

from energia_xm.tiempo_real import (
    enviar_eventos,
    escasez_desde_xm,
    eventos_desde_xm,
    parsear_cadena_conexion,
    solo_nuevos,
    token_sas,
)

CADENA = (
    "Endpoint=sb://esehxyz.servicebus.windows.net/;SharedAccessKeyName=key_123;"
    "SharedAccessKey=c2VjcmV0bw==;EntityPath=es_abc"
)


def respuesta_precio(fecha, valores):
    horas = {f"Hour{h:02d}": v for h, v in enumerate(valores, start=1)}
    return {"Items": [{"Date": f"{fecha}T00:00:00", "HourlyEntities": [{"Id": "Sistema", "Values": horas}]}]}


def test_parsear_cadena_conexion():
    c = parsear_cadena_conexion(CADENA)
    assert c == {"host": "esehxyz.servicebus.windows.net", "entidad": "es_abc",
                 "clave_nombre": "key_123", "clave": "c2VjcmV0bw=="}


def test_parsear_cadena_incompleta():
    with pytest.raises(ValueError, match="EntityPath"):
        parsear_cadena_conexion("Endpoint=sb://x/;SharedAccessKeyName=a;SharedAccessKey=b")


def test_token_sas_firma_verificable():
    uri = "https://esehxyz.servicebus.windows.net/es_abc"
    token = token_sas(uri, "key_123", "clave", segundos=60, ahora=1000)
    campos = dict(p.split("=", 1) for p in token.removeprefix("SharedAccessSignature ").split("&"))
    assert campos["se"] == "1060" and campos["skn"] == "key_123"
    recurso = urllib.parse.quote_plus(uri.lower())
    esperado = base64.b64encode(hmac.new(b"clave", f"{recurso}\n1060".encode(), hashlib.sha256).digest()).decode()
    assert urllib.parse.unquote_plus(campos["sig"]) == esperado


def test_eventos_desde_xm_horas_y_escasez():
    valores = ["100"] * 23 + ["900.5"]
    eventos = eventos_desde_xm(respuesta_precio("2024-09-30", valores), {"2024-09-30": 800.0})
    assert len(eventos) == 24
    primero, ultimo = eventos[0], eventos[-1]
    assert primero["hora"] == 0 and primero["ts_colombia"] == "2024-09-30T00:00:00"
    assert primero["ts_utc"] == "2024-09-30T05:00:00Z" and primero["sobre_escasez"] is False
    assert ultimo["hora"] == 23 and ultimo["ts_utc"] == "2024-10-01T04:00:00Z"
    assert ultimo["precio_bolsa_cop_kwh"] == 900.5 and ultimo["sobre_escasez"] is True


def test_eventos_omite_horas_vacias_y_sin_escasez():
    eventos = eventos_desde_xm(respuesta_precio("2026-10-17", ["200", ""] + [None] * 22), {})
    assert [e["hora"] for e in eventos] == [0]
    assert eventos[0]["precio_escasez_cop_kwh"] is None and eventos[0]["sobre_escasez"] is None


def test_escasez_desde_xm():
    resp = {"Items": [{"Date": "2026-10-01", "DailyEntities": [{"Id": "Sistema", "Value": "1012.3"}]},
                      {"Date": "2026-10-02", "DailyEntities": [{"Id": "Sistema", "Value": ""}]}]}
    assert escasez_desde_xm(resp) == {"2026-10-01": 1012.3}


def test_solo_nuevos():
    eventos = [{"ts_utc": "2026-10-17T05:00:00Z"}, {"ts_utc": "2026-10-17T06:00:00Z"}]
    assert solo_nuevos(eventos, None) == eventos
    assert solo_nuevos(eventos, "2026-10-17T05:00:00Z") == eventos[1:]


class Respuesta:
    def __init__(self, status_code, text=""):
        self.status_code, self.text = status_code, text


def test_enviar_eventos_un_mensaje_por_evento():
    llamadas = []

    def post(url, headers, data, timeout):
        llamadas.append((url, headers, json.loads(data)))
        return Respuesta(201)

    n = enviar_eventos([{"hora": 1}, {"hora": 2}], parsear_cadena_conexion(CADENA), post=post)
    assert n == 2
    assert llamadas[0][0] == "https://esehxyz.servicebus.windows.net/es_abc/messages?timeout=60&api-version=2014-01"
    assert llamadas[0][1]["Authorization"].startswith("SharedAccessSignature ")
    assert [c[2]["hora"] for c in llamadas] == [1, 2]


def test_enviar_eventos_falla_con_error_http():
    with pytest.raises(RuntimeError, match="401"):
        enviar_eventos([{"hora": 1}], parsear_cadena_conexion(CADENA), post=lambda *a, **k: Respuesta(401, "no"))
