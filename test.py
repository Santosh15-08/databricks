from pyspark.sql import SparkSession
from pyspark.sql.functions import when
spark=SparkSession.builder.appName("sample").master("local[2]").getOrCreate()
df_data = [
    (1, 'Alice', 45),
    (2, 'Bob', 120),
    (3, 'Charlie', 75),
    (4, 'David', 180),
    (5, 'Eve', 220)
]
df=spark.createDataFrame(df_data,["id","name","sales"])
df.show()
df_filter=df.filter(df.sales>50)
df_filter.show()
df_sales_category=df.withColumn("sales_category",when((df.sales>50) & (df.sales <=100),"Low")
                                                .when((df.sales>100) & (df.sales <=200),"Medium")
                                                .when(df.sales>200,"High")
                                                .otherwise("unknown"))
df_sales_category.show()
df_agg=df_sales_category.groupBy("sales_category").avg("sales").alias("avg_sales_in_each_category")
df_agg.show()