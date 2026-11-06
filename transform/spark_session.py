"""I keep the SparkSession setup in one place so every job starts Spark the same way."""
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(app_name: str = "overture-vn") -> SparkSession:
    """I create a local SparkSession with Delta Lake enabled."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")  # I run on all local cores since there is no cluster
        # These two settings turn on Delta Lake (MERGE, time travel) for all tables
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "4g")
        # I lower shuffle partitions from the default 200; that is far too many for
        # a single machine and mostly creates tiny tasks
        .config("spark.sql.shuffle.partitions", "8")
    )
    # I let delta-spark add the Delta jars that match the installed PySpark version
    return configure_spark_with_delta_pip(builder).getOrCreate()