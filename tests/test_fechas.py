import datetime as dt

import pytest

from energia_xm.fechas import archivos_en_rango, resolver_fechas, ruta_bronze, ventanas_mensuales

D = dt.date


def test_incremental_por_defecto_son_7_dias_hasta_hoy():
    assert resolver_fechas("", "", 7, hoy=D(2026, 10, 19)) == (D(2026, 10, 12), D(2026, 10, 19))


def test_backfill_con_start_date():
    assert resolver_fechas("2022-01-01", "", 7, hoy=D(2026, 10, 19)) == (D(2022, 1, 1), D(2026, 10, 19))


def test_rango_invertido_falla():
    with pytest.raises(ValueError):
        resolver_fechas("2026-10-20", "2026-10-01", 7)


def test_ventanas_parten_por_mes_calendario():
    assert ventanas_mensuales(D(2024, 1, 20), D(2024, 3, 5)) == [
        (D(2024, 1, 20), D(2024, 1, 31)),
        (D(2024, 2, 1), D(2024, 2, 29)),  # bisiesto
        (D(2024, 3, 1), D(2024, 3, 5)),
    ]


def test_backfill_completo_no_pasa_de_31_dias_ni_deja_huecos():
    ventanas = ventanas_mensuales(D(2022, 1, 1), D(2026, 10, 19))
    assert len(ventanas) == 58
    assert all((b - a).days + 1 <= 31 for a, b in ventanas)
    assert all(ventanas[i][1] + dt.timedelta(days=1) == ventanas[i + 1][0] for i in range(len(ventanas) - 1))


def test_ventana_de_un_dia():
    assert ventanas_mensuales(D(2026, 10, 19), D(2026, 10, 19)) == [(D(2026, 10, 19), D(2026, 10, 19))]


def test_ruta_bronze_es_idempotente():
    ruta = ruta_bronze("Gene", "Recurso", D(2024, 2, 1), D(2024, 2, 29))
    assert ruta == "Files/bronze/xm/Gene/Recurso/2024-02-01_2024-02-29.json"
    assert ruta == ruta_bronze("Gene", "Recurso", D(2024, 2, 1), D(2024, 2, 29))


def test_archivos_en_rango_toma_los_que_se_cruzan():
    archivos = [(n, f"x/{n}") for n in [
        "2026-08-01_2026-08-31.json",
        "2026-09-01_2026-09-30.json",
        "2026-10-01_2026-10-16.json",
        "2026-10-12_2026-10-19.json",
        "snapshot_2026-10-19.json",
    ]]
    assert archivos_en_rango(archivos, D(2026, 9, 30), D(2026, 10, 13)) == [
        "x/2026-09-01_2026-09-30.json", "x/2026-10-01_2026-10-16.json", "x/2026-10-12_2026-10-19.json"]
