from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.master("local[1]").getOrCreate()

df3 = spark.read.json("/home/bigdata/datalake/raw/twitter/20220501/*.json")
print(df3.show())

df3.withColumn("_id", df3.data.id).show()

users = df3.select(col('includes.users'))
users_alone = users.select(explode("users"))

u = users_alone.select(col('col.*'))