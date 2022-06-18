from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date, to_timestamp
import datetime
import os


spark = SparkSession.builder.appName('Twitter ETL users') \
        .master('local[*]') \
        .config("spark.executorEnv.JAVA_HOME", "/usr/lib/jvm/java-11-openjdk-amd64") \
        .config("spark.yarn.appMasterEnv.JAVA_HOME", "/usr/lib/jvm/java-11-openjdk-amd64") \
        .config("spark.mongodb.connection.uri",
                "mongodb+srv://remote_worker:remote_worker@bddbd.ptwl0.mongodb.net/bigdataproject") \
        .config("spark.mongodb.database", "bigdataproject") \
        .config("spark.mongodb.collection", "twitter.user") \
        .config("spark.mongodb.input.uri",
                "mongodb+srv://remote_worker:remote_worker@bddbd.ptwl0.mongodb.net/bigdataproject") \
        .config("spark.mongodb.output.uri",
                "mongodb+srv://remote_worker:remote_worker@bddbd.ptwl0.mongodb.net/bigdataproject") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.0') \
        .getOrCreate()

def users_to_mongo(date):

    path_to_files = os.sep.join(['home', 'bigdata', 'datalake', 'raw', 'twitter', date])
    if not os.path.exists('/' + path_to_files):
        return

    # Load raw
    tweets_raw = spark.read.json('/' + path_to_files)

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


if __name__ == '__main__':
    for i in range(1, 19):
        if i < 10:
            i = f'0{i}'
        date = f'202206{i}'
        print(f'Go for {date}')
        users_to_mongo(date)
        print(f'End of {date}')
