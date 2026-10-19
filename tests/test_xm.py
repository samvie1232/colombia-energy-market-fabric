import pytest
import requests

from energia_xm.xm import METRICAS, consultar_xm, contar_registros


class Respuesta:
    def __init__(self, status_code, cuerpo=None):
        self.status_code, self._cuerpo, self.text = status_code, cuerpo, str(cuerpo)

    def json(self):
        return self._cuerpo


def falso_post(respuestas):
    llamadas = []

    def post(url, json, timeout):
        llamadas.append(url)
        r = respuestas.pop(0)
        if isinstance(r, Exception):
            raise r
        return r

    return post, llamadas


def test_reintenta_y_luego_responde():
    post, llamadas = falso_post(
        [Respuesta(500, "error"), requests.ConnectionError("caída"), Respuesta(200, {"Items": []})])
    esperas = []
    assert consultar_xm("hourly", {"MetricId": "Gene"}, post=post, dormir=esperas.append) == {"Items": []}
    assert len(llamadas) == 3
    assert esperas == [2.0, 4.0]


def test_falla_tras_agotar_intentos():
    post, _ = falso_post([Respuesta(503, "no")] * 4)
    esperas = []
    with pytest.raises(RuntimeError, match="Gene tras 4 intentos: HTTP 503"):
        consultar_xm("hourly", {"MetricId": "Gene"}, post=post, dormir=esperas.append)
    assert esperas == [2.0, 4.0, 8.0]


def test_contar_registros_suma_entidades_de_todos_los_dias():
    respuesta = {"Items": [
        {"Date": "2026-10-15", "HourlyEntities": [{"Id": "Recurso"}, {"Id": "Recurso"}]},
        {"Date": "2026-10-16", "HourlyEntities": [{"Id": "Recurso"}]},
    ]}
    assert contar_registros(respuesta) == 3
    assert contar_registros({}) == 0


def test_metricas_del_contrato():
    esperadas = {"PrecBolsNaci", "DemaCome", "Gene", "VoluUtilDiarEner", "AporEner", "PrecEsca"}
    assert {m for m, _, _ in METRICAS} == esperadas
    assert all(endpoint in {"hourly", "daily"} for _, _, endpoint in METRICAS)
