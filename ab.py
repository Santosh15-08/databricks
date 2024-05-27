from pyspark.sql import SparkSession
spark=SparkSessoin.builder.appName("Sample").master("local[2]").getOrCreate()
print(spark)
