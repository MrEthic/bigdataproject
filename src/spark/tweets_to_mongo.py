from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date, to_timestamp
import datetime
import sys
import os


spark = SparkSession.builder.appName('TEST')\
    .master('local[*]')\
    .config("spark.executorEnv.JAVA_HOME", "/usr/lib/jvm/java-11-openjdk-amd64")\
    .config("spark.yarn.appMasterEnv.JAVA_HOME", "/usr/lib/jvm/java-11-openjdk-amd64")\
    .config("spark.mongodb.connection.uri", "mongodb+srv://remote_worker:remote_worker@bddbd.ptwl0.mongodb.net/bigdataproject")\
    .config("spark.mongodb.database", "bigdataproject")\
    .config("spark.mongodb.collection", "twitter.tweet")\
    .config("spark.mongodb.input.uri", "mongodb+srv://remote_worker:remote_worker@bddbd.ptwl0.mongodb.net/bigdataproject")\
    .config("spark.mongodb.output.uri", "mongodb+srv://remote_worker:remote_worker@bddbd.ptwl0.mongodb.net/bigdataproject")\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector:10.0.0')\
    .getOrCreate()


def datalake_to_mongo(date):
    # Load raw
    tweets_raw = spark.read.json(f"/home/bigdata/datalake/raw/twitter/{date}/*.json")

    # Extract Extract .withColumn("created_at", to_timestamp('created_at_')) \
    tweets = tweets_raw \
        .select(col('data.*')) \
        .drop(col('attachments')) \
        .withColumnRenamed("created_at", "created_at_") \
        .withColumn("created_at", to_timestamp('created_at_'))\
        .withColumnRenamed("id", "_id") \
        .drop(col('created_at_')) \
        .drop(col('geo'))

    tweets.write \
        .format("mongodb") \
        .mode("append") \
        .option("database", "bigdataproject") \
        .option("collection", "twitter.tweet") \
        .save()

    #tweets.show()


def main():
    days = os.listdir("/home/bigdata/datalake/raw/twitter/")
    for day in days:
        print(f"Processing {day}")
        datalake_to_mongo(day)
        print(f"Finished {day}")


if __name__ == '__main__':
    t1 = datetime.datetime.now()
    print('Started at :', t1)
    main()
    t2 = datetime.datetime.now()
    dist = t2 - t1
    print(f'Finished at: {t2} | elapsed time {dist.seconds}s')
    #spark.sparkContext._gateway.close()
    spark.stop()
    #sys.exit(0)

#df = spark.read.format("mongodb").load()





