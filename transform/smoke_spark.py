from delta.tables import DeltaTable

from spark_session import get_spark

spark = get_spark("smoke")

# 1) Write and read a Delta table
path = "data/tmp/smoke_delta"
spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"]) \
    .write.format("delta").mode("overwrite").save(path)
spark.read.format("delta").load(path).show()

# 2) Test MERGE, the core operation for SCD2 later
updates = spark.createDataFrame([(2, "b2"), (3, "c")], ["id", "val"])
(DeltaTable.forPath(spark, path).alias("t")
    .merge(updates.alias("u"), "t.id = u.id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())
spark.read.format("delta").load(path).orderBy("id").show()

# 3) Read the bronze Parquet from Phase 0
bronze = spark.read.parquet("data/bronze/places/release=*/part-0.parquet")
print("bronze rows:", bronze.count())
bronze.select("id", "names.primary", "basic_category", "confidence").show(5, truncate=False)
bronze.printSchema()

spark.stop()