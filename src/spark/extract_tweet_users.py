from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp
import datetime


spark = SparkSession \
        .builder \
        .appName("extract_tweet_users") \
        .config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector:10.0.1") \
        .config("spark.mongodb.input.uri", "mongodb+srv://remote_worker:remote_worker@bddbd.ptwl0.mongodb.net/") \
        .config("spark.mongodb.output.uri", "mongodb+srv://remote_worker:remote_worker@bddbd.ptwl0.mongodb.net/") \
        .config("spark.executorEnv.JAVA_HOME", "/usr/lib/jvm/java-11-openjdk-amd64") \
        .config("spark.yarn.appMasterEnv.JAVA_HOME", "/usr/lib/jvm/java-11-openjdk-amd64") \
        .config("spark.mongodb.connection.uri", "mongodb+srv://remote_worker:remote_worker@bddbd.ptwl0.mongodb.net/") \
        .config("spark.executor.memory", "10g") \
        .master("local[*]") \
        .getOrCreate()

def datalake_to_mongo():
    # Load raw
    tweets_raw = spark.read.json("/home/bigdata/datalake/raw/twitter/20220501/*.json")

    # Extract Extract
    users = tweets_raw.select(col('includes.users'))\
        .select(explode("users"))\
        .select(col('col.*')) \
        .withColumnRenamed("created_at", "created_at_") \
        .withColumn("created_at",to_timestamp('created_at_'))\
        .withColumnRenamed("id","_id")\
        .dropDuplicates(["_id"])\
        .drop(col('entities'))\
        .drop(col('created_at_'))

    users.write\
        .format("mongodb")\
        .mode("append")\
        .option("database","bigdataproject")\
        .option("collection", "twitter.user")\
        .save()

    users.show()


def main():
    datalake_to_mongo()


if __name__ == '__main__':
    t1 = datetime.datetime.now()
    print('Started at :', t1)
    main()
    t2 = datetime.datetime.now()
    dist = t2 - t1
    print(f'Finished at: {t2} | elapsed time {dist.seconds}s')
    #spark.sparkContext._gateway.close()
    #spark.stop()
    #exit(0)

#df = spark.read.format("mongodb").load()





