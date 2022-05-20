from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

# Create SparkSession
spark = SparkSession.builder.appName('TEST').\
    master('local[1]').\
    config("spark.mongodb.input.uri", "mongodb+srv://remote_worker:remote_worker@bddbd.ptwl0.mongodb.net/bigdataproject").\
    config("spark.mongodb.output.uri", "mongodb+srv://remote_worker:remote_worker@bddbd.ptwl0.mongodb.net/bigdataproject").\
    config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector:10.0.0').\
    config("spark.mongodb.connection.uri","mongodb+srv://remote_worker:remote_worker@bddbd.ptwl0.mongodb.net/bigdataproject").\
    config("spark.mongodb.database", "bigdataproject").\
    config("spark.mongodb.collection", "twitter.tweet").\
    getOrCreate()

# Load raw
tweets_raw = spark.read.json("/home/bigdata/datalake/raw/twitter/20220501/*.json")

# Extract Extract
users = tweets_raw.select(col('includes.users'))\
    .select(explode("users"))\
    .select(col('col.*'))\
    .withColumnRenamed("id","_id")\
    .dropDuplicates(["_id"])\
    .drop(col('entities'))


users.show()

users.write\
    .format("mongodb")\
    .mode("append")\
    .option("database","bigdataproject")\
    .option("collection", "twitter.user")\
    .save()




#df = spark.read.format("mongodb").load()





