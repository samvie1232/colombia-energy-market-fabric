import pytest


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")
    spark = (pyspark.sql.SparkSession.builder.master("local[1]")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    yield spark
    spark.stop()
