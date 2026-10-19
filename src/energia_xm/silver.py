"""Transformaciones Bronze → Silver en PySpark (las mismas del notebook nb_silver_xm)."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

TZ_XM = "America/Bogota"
HORAS = [f"Hour{h:02d}" for h in range(1, 25)]

# Esquemas explícitos: el sobre de Bronze tiene una clave "_metadata" que choca con la columna oculta
# del mismo nombre de Spark, por eso se lee como texto y se parsea con from_json.
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


def a_decimal(col: str):
    """Texto de la API a decimal(18,4); vacío o no numérico queda null, nunca 0."""
    return F.expr(f"try_cast(nullif(trim({col}), '') AS decimal(18,4))")


def leer_bronze(spark: SparkSession, rutas: list[str], esquema: T.StructType) -> DataFrame:
    """Una fila por Item (día) con la ruta y la marca de ingesta del archivo de origen."""
    return (spark.read.text(rutas, wholetext=True)
            .select(F.from_json("value", esquema).alias("j"), F.col("_metadata.file_path").alias("archivo_origen"))
            .select("archivo_origen",
                    F.col("j._metadata.run_id").alias("run_id_bronze"),
                    F.to_timestamp("j._metadata.ingestado_utc").alias("ingestado_utc"),
                    F.explode("j.respuesta.Items").alias("item")))


def transformar_horaria(spark: SparkSession, rutas: list[str], valor: str, con_recurso: bool) -> DataFrame:
    """Despivota Hour01..Hour24 a (fecha, hora 0..23) con ts_colombia y ts_utc."""
    df = (leer_bronze(spark, rutas, ESQUEMA_HORARIO)
          .select("*", F.to_date(F.substring("item.Date", 1, 10)).alias("fecha"),
                  F.explode("item.HourlyEntities").alias("e"))
          .select("archivo_origen", "run_id_bronze", "ingestado_utc", "fecha",
                  F.col("e.Values.code").alias("codigo_recurso"),
                  F.posexplode(F.array(*[F.col(f"e.Values.{h}") for h in HORAS])).alias("hora", "v"))
          .withColumn(valor, a_decimal("v"))
          .withColumn("ts_colombia", F.to_timestamp(
              F.format_string("%s %02d:00:00", F.col("fecha").cast("string"), F.col("hora"))))
          .withColumn("ts_utc", F.to_utc_timestamp("ts_colombia", TZ_XM)))
    columnas = ["fecha", "hora"] + (["codigo_recurso"] if con_recurso else []) + ["ts_colombia", "ts_utc", valor]
    return df.select(*columnas, "ingestado_utc", "run_id_bronze", "archivo_origen")


def transformar_diaria(spark: SparkSession, rutas: list[str], valor: str, con_embalse: bool) -> DataFrame:
    """Una fila por día (y por embalse si aplica); el nombre del embalse va en mayúsculas sin espacios sobrantes."""
    df = (leer_bronze(spark, rutas, ESQUEMA_DIARIO)
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
