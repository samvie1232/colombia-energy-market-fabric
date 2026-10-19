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

# # nb_silver_xm
# 
# Transforma el JSON crudo de **Bronze** en tablas Delta limpias del esquema `silver` del Lakehouse `lh_energia`.
# 
# - **Despivota** `Hour01..Hour24` a filas `(fecha, hora, valor)`. `Hour01` es la hora 00:00–01:00, así que `hora` va de 0 a 23.
# - **Zona horaria:** XM publica en hora de Colombia (UTC−5, sin horario de verano). Se guardan `ts_colombia` y `ts_utc`.
# - **Deduplica:** la misma fecha puede venir en varios archivos (ventana mensual del backfill y ventanas incrementales);
#   gana el archivo ingerido más reciente.
# - **MERGE** en Delta sobre la llave natural: las reliquidaciones de XM actualizan el valor y nada se duplica.
# - **Calidad:** nulos, duplicados y horas faltantes; el resultado de cada tabla queda en `silver_carga_log`.
# - **Incremental:** sin parámetros procesa los últimos 7 días. **Backfill:** `start_date="2022-01-01"`.
# 
# Contrato de datos: `docs/data_contract.md`.

# PARAMETERS CELL ********************

# Parámetros (se pueden sobrescribir desde el pipeline de Data Factory)
start_date = ""        # "YYYY-MM-DD"; vacío = hoy - dias_incremental
end_date = ""          # "YYYY-MM-DD"; vacío = hoy
dias_incremental = 7
metricas = ""          # vacío = todas; o lista separada por comas, p. ej. "PrecBolsNaci,Gene"
incluir_recursos = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import datetime as dt
import json
import re
import time
import uuid

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

BRONZE_ROOT = "Files/bronze/xm"
ESQUEMA = "silver"
TZ_XM = "America/Bogota"
spark.conf.set("spark.sql.session.timeZone", "UTC")

# MetricId, cruce, tipo, tabla Silver, columna de valor, llave natural (sin contar fecha)
METRICAS = [
    ("PrecBolsNaci", "Sistema", "hourly", "precio_bolsa_hora", "precio_cop_kwh", ["hora"]),
    ("DemaCome", "Sistema", "hourly", "demanda_hora", "demanda_kwh", ["hora"]),
    ("Gene", "Recurso", "hourly", "generacion_recurso_hora", "generacion_kwh", ["hora", "codigo_recurso"]),
    ("VoluUtilDiarEner", "Embalse", "daily", "embalse_dia", "volumen_util_kwh", ["embalse"]),
    ("AporEner", "Sistema", "daily", "aportes_dia", "aportes_kwh", []),
    ("PrecEsca", "Sistema", "daily", "precio_escasez_dia", "precio_escasez_cop_kwh", []),
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def resolver_fechas(start_date: str, end_date: str, dias_incremental: int, hoy: dt.date | None = None) -> tuple[dt.date, dt.date]:
    """Convierte los parámetros en un rango de fechas válido (igual que en Bronze)."""
    hoy = hoy or dt.date.today()
    fin = dt.date.fromisoformat(end_date) if end_date else hoy
    inicio = dt.date.fromisoformat(start_date) if start_date else fin - dt.timedelta(days=dias_incremental)
    if inicio > fin:
        raise ValueError(f"start_date {inicio} es posterior a end_date {fin}")
    return inicio, fin


PATRON_ARCHIVO = re.compile(r"(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})\.json$")


def archivos_en_rango(nombres_y_rutas: list[tuple[str, str]], inicio: dt.date, fin: dt.date) -> list[str]:
    """Rutas cuya ventana {ini}_{fin}.json se cruza con [inicio, fin]."""
    rutas = []
    for nombre, ruta in nombres_y_rutas:
        m = PATRON_ARCHIVO.search(nombre)
        if m and dt.date.fromisoformat(m.group(1)) <= fin and dt.date.fromisoformat(m.group(2)) >= inicio:
            rutas.append(ruta)
    return sorted(rutas)


def listar_bronze(metrica: str, cruce: str) -> list[tuple[str, str]]:
    carpeta = f"{BRONZE_ROOT}/{metrica}/{cruce}"
    try:
        return [(f.name, f.path) for f in notebookutils.fs.ls(carpeta)]
    except Exception:
        return []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Esquemas explícitos: el sobre de Bronze tiene una clave "_metadata" que choca con la columna oculta
# del mismo nombre de Spark, por eso se lee como texto y se parsea con from_json.
HORAS = [f"Hour{h:02d}" for h in range(1, 25)]
ESQUEMA_META = T.StructType([T.StructField("run_id", T.StringType()), T.StructField("ingestado_utc", T.StringType())])
ESQUEMA_HORARIO = T.StructType([
    T.StructField("_metadata", ESQUEMA_META),
    T.StructField("respuesta", T.StructType([T.StructField("Items", T.ArrayType(T.StructType([
        T.StructField("Date", T.StringType()),
        T.StructField("HourlyEntities", T.ArrayType(T.StructType([
            T.StructField("Id", T.StringType()),
            T.StructField("Values", T.StructType(
                [T.StructField("code", T.StringType())] + [T.StructField(h, T.StringType()) for h in HORAS])),
        ]))),
    ])))])),
])
ESQUEMA_DIARIO = T.StructType([
    T.StructField("_metadata", ESQUEMA_META),
    T.StructField("respuesta", T.StructType([T.StructField("Items", T.ArrayType(T.StructType([
        T.StructField("Date", T.StringType()),
        T.StructField("DailyEntities", T.ArrayType(T.StructType([
            T.StructField("Id", T.StringType()),
            T.StructField("Name", T.StringType()),
            T.StructField("Code", T.StringType()),
            T.StructField("Value", T.StringType()),
        ]))),
    ])))])),
])


def a_decimal(col):
    """Texto de la API a decimal(18,4); vacío o no numérico queda null, nunca 0."""
    return F.expr(f"try_cast(nullif(trim({col}), '') AS decimal(18,4))")


def leer_bronze(rutas: list[str], esquema: T.StructType) -> DataFrame:
    return (spark.read.text(rutas, wholetext=True)
            .select(F.from_json("value", esquema).alias("j"), F.col("_metadata.file_path").alias("archivo_origen"))
            .select("archivo_origen",
                    F.col("j._metadata.run_id").alias("run_id_bronze"),
                    F.to_timestamp("j._metadata.ingestado_utc").alias("ingestado_utc"),
                    F.explode("j.respuesta.Items").alias("item")))


def transformar_horaria(rutas: list[str], valor: str, con_recurso: bool) -> DataFrame:
    df = (leer_bronze(rutas, ESQUEMA_HORARIO)
          .select("*", F.to_date(F.substring("item.Date", 1, 10)).alias("fecha"),
                  F.explode("item.HourlyEntities").alias("e"))
          .select("archivo_origen", "run_id_bronze", "ingestado_utc", "fecha",
                  F.col("e.Values.code").alias("codigo_recurso"),
                  F.posexplode(F.array(*[F.col(f"e.Values.{h}") for h in HORAS])).alias("hora", "v"))
          .withColumn(valor, a_decimal("v"))
          .withColumn("ts_colombia", F.to_timestamp(F.format_string("%s %02d:00:00", F.col("fecha").cast("string"), F.col("hora"))))
          .withColumn("ts_utc", F.to_utc_timestamp("ts_colombia", TZ_XM)))
    columnas = ["fecha", "hora"] + (["codigo_recurso"] if con_recurso else []) + ["ts_colombia", "ts_utc", valor]
    return df.select(*columnas, "ingestado_utc", "run_id_bronze", "archivo_origen")


def transformar_diaria(rutas: list[str], valor: str, con_embalse: bool) -> DataFrame:
    df = (leer_bronze(rutas, ESQUEMA_DIARIO)
          .select("*", F.to_date(F.substring("item.Date", 1, 10)).alias("fecha"),
                  F.explode("item.DailyEntities").alias("e"))
          .withColumn("embalse", F.upper(F.trim(F.coalesce("e.Name", "e.Code", "e.Id"))))
          .withColumn(valor, a_decimal("e.Value")))
    columnas = ["fecha"] + (["embalse"] if con_embalse else []) + [valor]
    return df.select(*columnas, "ingestado_utc", "run_id_bronze", "archivo_origen")


def deduplicar(df: DataFrame, llave: list[str]) -> DataFrame:
    """Una fila por llave: gana la ingesta más reciente (y, a igualdad, el archivo de nombre mayor)."""
    w = Window.partitionBy(*llave).orderBy(F.col("ingestado_utc").desc_nulls_last(), F.col("archivo_origen").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def merge_delta(df: DataFrame, tabla: str, llave: list[str], valor: str) -> dict:
    """Crea la tabla si no existe; si existe, MERGE por la llave natural. Devuelve filas insertadas/actualizadas."""
    nombre = f"{ESQUEMA}.{tabla}"
    df = df.withColumn("actualizado_utc", F.current_timestamp())
    if not spark.catalog.tableExists(nombre):
        df.write.format("delta").saveAsTable(nombre)
        return {"insertadas": spark.table(nombre).count(), "actualizadas": 0}
    destino = DeltaTable.forName(spark, nombre)
    cond = " AND ".join(f"t.{c} = s.{c}" for c in llave)
    (destino.alias("t").merge(df.alias("s"), cond)
     # Solo se reescribe la fila si el valor cambió (reliquidación); recargar lo mismo no genera versiones inútiles.
     .whenMatchedUpdateAll(condition=f"NOT (t.{valor} <=> s.{valor})")
     .whenNotMatchedInsertAll()
     .execute())
    m = destino.history(1).select("operationMetrics").first()[0] or {}
    return {"insertadas": int(m.get("numTargetRowsInserted", 0)), "actualizadas": int(m.get("numTargetRowsUpdated", 0))}


def validar(tabla: str, llave: list[str], valor: str, inicio: dt.date, fin: dt.date, completitud_24h: bool) -> dict:
    """Chequeos sobre la tabla ya cargada, limitados al rango procesado."""
    df = spark.table(f"{ESQUEMA}.{tabla}").filter(F.col("fecha").between(F.lit(inicio), F.lit(fin)))
    r = {
        "nulos_llave": df.filter(" OR ".join(f"{c} IS NULL" for c in llave)).count(),
        "nulos_valor": df.filter(F.col(valor).isNull()).count(),
        "duplicados": df.groupBy(*llave).count().filter("count > 1").count(),
        "dias_incompletos": 0,
    }
    if completitud_24h:
        r["dias_incompletos"] = df.groupBy("fecha").agg(F.countDistinct("hora").alias("h")).filter("h <> 24").count()
    return r

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def cargar_recursos() -> int:
    """silver.recurso desde el snapshot más reciente de ListadoRecursos (sobrescribe la tabla)."""
    snapshots = sorted(f.name for f in notebookutils.fs.ls(f"{BRONZE_ROOT}/ListadoRecursos/Sistema") if f.name.endswith(".json"))
    if not snapshots:
        print("Sin snapshots de ListadoRecursos; se omite silver.recurso")
        return 0
    ruta = f"{BRONZE_ROOT}/ListadoRecursos/Sistema/{snapshots[-1]}"
    with open(f"/lakehouse/default/{ruta}", encoding="utf-8") as fh:
        sobre = json.load(fh)
    # Se aplanan los Values tal cual (todo como texto); en Gold se eligen y tipan las columnas útiles.
    filas = []
    for item in sobre["respuesta"].get("Items", []):
        for ent in item.get("ListEntities", []):
            plano = {"id": ent.get("Id")} | {k: v for k, v in (ent.get("Values") or {}).items()}
            filas.append({re.sub(r"[^0-9a-zA-Z_]", "_", k).lower(): (None if v is None else str(v)) for k, v in plano.items()})
    columnas = list(dict.fromkeys(k for f in filas for k in f))
    print(f"ListadoRecursos {snapshots[-1]}: {len(filas)} recursos, columnas {columnas}")
    esquema = T.StructType([T.StructField(c, T.StringType()) for c in columnas])
    (spark.createDataFrame([tuple(f.get(c) for c in columnas) for f in filas], esquema)
     .withColumn("snapshot", F.lit(snapshots[-1].removeprefix("snapshot_").removesuffix(".json")).cast("date"))
     .withColumn("actualizado_utc", F.current_timestamp())
     .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{ESQUEMA}.recurso"))
    return len(filas)

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
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ESQUEMA}")

bitacora = []
for metrica, cruce, tipo, tabla, valor, llave_extra in a_cargar:
    t0 = time.time()
    llave = ["fecha"] + llave_extra
    fila = {"run_id": run_id, "metrica": metrica, "tabla": f"{ESQUEMA}.{tabla}", "rango_inicio": inicio, "rango_fin": fin,
            "archivos": 0, "filas_lote": 0, "insertadas": 0, "actualizadas": 0, "nulos_llave": 0, "nulos_valor": 0,
            "duplicados": 0, "dias_incompletos": 0, "estado": "ok", "error": None}
    try:
        rutas = archivos_en_rango(listar_bronze(metrica, cruce), inicio, fin)
        fila["archivos"] = len(rutas)
        if not rutas:
            fila["estado"] = "sin_datos"
        else:
            if tipo == "hourly":
                df = transformar_horaria(rutas, valor, con_recurso="codigo_recurso" in llave)
            else:
                df = transformar_diaria(rutas, valor, con_embalse="embalse" in llave)
            df = deduplicar(df.filter(F.col("fecha").between(F.lit(inicio), F.lit(fin))), llave).cache()
            fila["filas_lote"] = df.count()
            fila.update(merge_delta(df, tabla, llave, valor))
            df.unpersist()
            fila.update(validar(tabla, llave, valor, inicio, fin, completitud_24h=(tipo == "hourly" and cruce == "Sistema")))
            if fila["duplicados"] or fila["nulos_llave"]:
                fila["estado"] = "error"
                fila["error"] = "llave duplicada o nula"
            elif fila["nulos_valor"] or fila["dias_incompletos"]:
                fila["estado"] = "alerta"
    except Exception as exc:  # se registra y se sigue con la siguiente tabla
        fila["estado"], fila["error"] = "error", str(exc)[:500]
    fila["segundos"] = round(time.time() - t0, 2)
    fila["cargado_utc"] = dt.datetime.now(dt.timezone.utc)
    bitacora.append(fila)
    print(f"{fila['estado']:9} {tabla:25} archivos={fila['archivos']:3} lote={fila['filas_lote']:9} "
          f"+{fila['insertadas']} ~{fila['actualizadas']} nulos={fila['nulos_valor']} incompletos={fila['dias_incompletos']}")

if incluir_recursos and (not seleccion or "ListadoRecursos" in seleccion):
    cargar_recursos()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

esquema_log = T.StructType([
    T.StructField("run_id", T.StringType()), T.StructField("metrica", T.StringType()), T.StructField("tabla", T.StringType()),
    T.StructField("rango_inicio", T.DateType()), T.StructField("rango_fin", T.DateType()),
    T.StructField("archivos", T.LongType()), T.StructField("filas_lote", T.LongType()),
    T.StructField("insertadas", T.LongType()), T.StructField("actualizadas", T.LongType()),
    T.StructField("nulos_llave", T.LongType()), T.StructField("nulos_valor", T.LongType()),
    T.StructField("duplicados", T.LongType()), T.StructField("dias_incompletos", T.LongType()),
    T.StructField("estado", T.StringType()), T.StructField("error", T.StringType()),
    T.StructField("segundos", T.DoubleType()), T.StructField("cargado_utc", T.TimestampType()),
])
spark.createDataFrame(bitacora, esquema_log).write.mode("append").saveAsTable("silver_carga_log")

errores = [b for b in bitacora if b["estado"] == "error"]
print(f"Tablas: {len(bitacora)}  ok: {sum(b['estado'] == 'ok' for b in bitacora)}  "
      f"alertas: {sum(b['estado'] == 'alerta' for b in bitacora)}  errores: {len(errores)}")
if errores:
    # Falla el notebook para que el pipeline lo marque; las tablas que salieron bien ya quedaron escritas.
    raise RuntimeError("Fallaron: " + "; ".join(f"{e['tabla']}: {e['error']}" for e in errores))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
