from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date, to_timestamp
import datetime
import os


def users_to_mongo():
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

    today = datetime.date.today()
    today_folder = today.strftime("%Y%m%d")

    path_to_files = os.sep.join(['home', 'bigdata', 'datalake', 'raw', 'twitter', today_folder])
    if not os.path.exists('/' + path_to_files):
        raise ValueError(f"{path_to_files} doesn't exists")

    # Load raw
    tweets_raw = spark.read.json(path_to_files)

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

users_to_mongo()