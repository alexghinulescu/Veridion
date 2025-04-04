from pyspark.sql import SparkSession
from pyspark.sql import functions as func


spark = SparkSession.builder.appName("Parquet Duplicates").getOrCreate()
df = spark.read.parquet('file:///C:/Veridion/data/veridion_product_deduplication_challenge.snappy.parquet')


def unite(col_name, dtype):
    if dtype == 'array<string>':
        return func.flatten(func.collect_set(col_name)).alias(col_name)
    else:
        return func.first(col_name).alias(col_name)


df = df.withColumn(
    "price",
    func.expr("array_distinct(transform(price, x -> concat_ws(',', x.amount, x.currency, x.type)))")
).withColumn(
    "production_capacity",
    func.expr("array_distinct(transform(production_capacity, x -> concat_ws(',', x.quantity, x.time_frame, x.type, x.unit)))")
).withColumn(
    "size",
    func.expr("array_distinct(transform(size, x -> concat_ws(',', x.dimension, x.qualitative, x.type, x.unit, x.value)))")
).withColumn(
    "color",
    func.expr("array_distinct(transform(color, x -> concat_ws(',', x.original, x.simple)))")
).withColumn(
    "purity",
    func.expr("array_distinct(transform(purity, x -> concat_ws(',', x.qualitative, x.type, x.unit, x.value)))")
)


col_wduplicate = [col for col in df.columns if col != 'product_title']

agg_cols = [unite(col, dtype) for col, dtype in df.dtypes if col != 'product_title']

no_duplicate = df.groupBy('product_title').agg(*agg_cols)
no_duplicate.coalesce(1).write.mode("overwrite").parquet("file:///C:/Veridion/no_duplicate_output.parquet")
no_duplicate.show(truncate=False)
spark.stop()
