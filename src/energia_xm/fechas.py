"""Rangos de fechas, ventanas de consulta y rutas de Bronze."""

from __future__ import annotations

import datetime as dt
import re

BRONZE_ROOT = "Files/bronze/xm"
MAX_DIAS_POR_PETICION = 31
PATRON_ARCHIVO = re.compile(r"(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})\.json$")


def resolver_fechas(
    start_date: str, end_date: str, dias_incremental: int, hoy: dt.date | None = None
) -> tuple[dt.date, dt.date]:
    """Convierte los parámetros del notebook en un rango válido.

    Vacío en ``end_date`` es hoy; vacío en ``start_date`` es ``end_date - dias_incremental``.
    """
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
    return ventanas


def ruta_bronze(metrica: str, cruce: str, inicio: dt.date, fin: dt.date) -> str:
    """Ruta idempotente: la misma ventana siempre cae en el mismo archivo."""
    return f"{BRONZE_ROOT}/{metrica}/{cruce}/{inicio:%Y-%m-%d}_{fin:%Y-%m-%d}.json"


def archivos_en_rango(nombres_y_rutas: list[tuple[str, str]], inicio: dt.date, fin: dt.date) -> list[str]:
    """Rutas cuya ventana ``{ini}_{fin}.json`` se cruza con [inicio, fin]."""
    rutas = []
    for nombre, ruta in nombres_y_rutas:
        m = PATRON_ARCHIVO.search(nombre)
        if m and dt.date.fromisoformat(m.group(1)) <= fin and dt.date.fromisoformat(m.group(2)) >= inicio:
            rutas.append(ruta)
    return sorted(rutas)
