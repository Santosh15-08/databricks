from pyspark.sql import SparkSession
spark = (SparkSession.builder.master('local') \
         .appName('spark-read-from-bigquery') \
         .config("spark.jars","/Users/santo/Downloads/spark-bigquery-latest_2.12.jar") \
         .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
         .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/Users/santo/Downloads/bdb-proj-afa0cc1b575a.json") \
         .getOrCreate())

def read_query_bigquery(project, query):
    df = spark.read.format('bigquery') \
        .option("parentProject", "{project}".format(project=project)) \
        .option('query', query) \
        .option('viewsEnabled', 'true') \
        .load()
    return df

project = 'bdb-proj'
query = 'select * from bdb-proj.test_db.employees'
spark.conf.set("materializationDataset", 'test_db')
df = read_query_bigquery(project, query)
df.show()