from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Parquet Duplicates").getOrCreate()

df1 = spark.read.parquet("file:///C:/Veridion/data/veridion_product_deduplication_challenge.snappy.parquet")
df2 = spark.read.parquet("file:///C:/Veridion/no_duplicate_output.parquet")

df1.printSchema()
df2.printSchema()
print("Columns in df1:", df1.columns)
print("Columns in df2:", df2.columns)
df1.show(5, truncate=False)
df2.show(5, truncate=False)
spark.stop()

