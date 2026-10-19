import json
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")
from energia_xm.silver import deduplicar, transformar_diaria, transformar_horaria  # noqa: E402

pytestmark = pytest.mark.spark


def escribir(tmp_path, nombre, items, ingestado):
    ruta = tmp_path / nombre
    ruta.write_text(json.dumps({"_metadata": {"run_id": "r", "ingestado_utc": ingestado, "fuente": "x"},
                                "respuesta": {"Items": items}}))
    return str(ruta)


def horario(fecha, codigo, valor=lambda h: 100 + h):
    valores = {"code": codigo} | {f"Hour{h:02d}": valor(h) for h in range(1, 25)}
    return {"Date": fecha, "HourlyEntities": [{"Id": "Sistema", "Values": valores}]}


def test_despivota_24_horas_con_hora_colombia_y_utc(spark, tmp_path):
    ruta = escribir(tmp_path, "2026-10-15_2026-10-15.json", [horario("2026-10-15", "Sistema")],
                    "2026-10-18T10:00:00+00:00")
    df = transformar_horaria(spark, [ruta], "precio_cop_kwh", con_recurso=False)
    filas = {r.hora: r for r in df.collect()}
    assert sorted(filas) == list(range(24))
    assert filas[0].precio_cop_kwh == Decimal("101.0000")   # Hour01 = 00:00–01:00
    assert str(filas[5].ts_colombia) == "2026-10-15 05:00:00"
    assert str(filas[5].ts_utc) == "2026-10-15 10:00:00"     # UTC−5, sin horario de verano


def test_valores_vacios_quedan_null_nunca_cero(spark, tmp_path):
    item = horario("2026-10-15", "3ACC", valor=lambda h: "" if h > 18 else str(h))
    ruta = escribir(tmp_path, "a_2026-10-15_2026-10-15.json", [item], "2026-10-18T10:00:00+00:00")
    df = transformar_horaria(spark, [ruta], "generacion_kwh", con_recurso=True)
    assert df.filter("generacion_kwh IS NULL").count() == 6
    assert df.filter("generacion_kwh = 0").count() == 0
    assert {r.codigo_recurso for r in df.collect()} == {"3ACC"}


def test_deduplicar_gana_la_ingesta_mas_reciente(spark, tmp_path):
    viejo = escribir(tmp_path, "2026-10-01_2026-10-16.json", [horario("2026-10-12", "Sistema")],
                     "2026-10-17T10:00:00+00:00")
    nuevo = escribir(tmp_path, "2026-10-12_2026-10-19.json",
                     [horario("2026-10-12", "Sistema", valor=lambda h: 999.5 if h == 6 else 100 + h)],
                     "2026-10-19T10:00:00+00:00")
    df = deduplicar(transformar_horaria(spark, [viejo, nuevo], "precio_cop_kwh", con_recurso=False), ["fecha", "hora"])
    assert df.count() == 24
    assert df.filter("hora = 5").first().precio_cop_kwh == Decimal("999.5000")


def test_diaria_por_embalse_normaliza_nombre(spark, tmp_path):
    items = [{"Date": "2026-10-18T00:00:00", "DailyEntities": [
        {"Id": "Embalse", "Name": "Altoanchicaya ", "Value": 1234.5},
        {"Id": "Embalse", "Name": "AMANI", "Value": ""}]}]
    ruta = escribir(tmp_path, "2026-10-18_2026-10-18.json", items, "2026-10-19T10:00:00+00:00")
    filas = {r.embalse: r for r in transformar_diaria(spark, [ruta], "volumen_util_kwh", con_embalse=True).collect()}
    assert set(filas) == {"ALTOANCHICAYA", "AMANI"}
    assert filas["ALTOANCHICAYA"].volumen_util_kwh == Decimal("1234.5000")
    assert filas["AMANI"].volumen_util_kwh is None
    assert str(filas["AMANI"].fecha) == "2026-10-18"
