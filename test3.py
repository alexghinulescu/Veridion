from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql import functions as func


spark = SparkSession.builder.appName("Parquet Duplicates").getOrCreate()

df = spark.read.parquet('file:///C:/Veridion/data/veridion_product_deduplication_challenge.snappy.parquet')


schema = df.schema
df.groupBy("product_title").agg(count("*").alias("count")).show(truncate=False)


def unite(col_name):
    return func.concat_ws(', ', func.collect_set(col_name)).alias(col_name)


duplicate = [col for col, dtype in df.dtypes if dtype in ('string', 'array') and col != 'product_title']
no_duplicate = df.groupBy('product_title').agg(*[unite(col) for col in duplicate])
no_duplicate.show(truncate=False)
no_duplicate.coalesce(1).write.mode("overwrite").parquet("file:///C:/Veridion/no_duplicate_output.parquet")

spark.stop()
