from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

# Create SparkSession
spark = SparkSession.builder.master("local[1]").getOrCreate()

tweets_raw = spark.read.json("/home/bigdata/datalake/raw/twitter/20220501/*.json")
users_raw = tweets_raw.select(col('includes.users'))
users_alone = users_raw.select(explode("users"))
users_expanded = users_alone.select(col('col.*'))

print(users_expanded.show())

#df3.withColumn("_id", df3.data.id).show()




