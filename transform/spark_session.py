"""Shared SparkSession factory so every job starts Spark with the same settings."""
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(app_name: str = "overture-vn") -> SparkSession:
    """Create a local SparkSession with Delta Lake enabled."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        # Enables Delta features (MERGE, time travel) for all tables
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "4g")
        # Default of 200 shuffle partitions only creates tiny tasks on one machine
        .config("spark.sql.shuffle.partitions", "8")
    )
    # delta-spark picks the Delta jars matching the installed PySpark version
    return configure_spark_with_delta_pip(builder).getOrCreate()