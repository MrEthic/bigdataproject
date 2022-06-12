from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date, to_timestamp
import datetime
import sys

#spark-submit --master local[*] --conf spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 --conf spark.yarn.appMasterEnv.JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 --conf spark.mongodb.input.uri=mongodb+srv://remote_worker:remote_worker@bddbd.ptwl0.mongodb.net/bigdataproject --conf spark.mongodb.output.uri=mongodb+srv://remote_worker:remote_worker@bddbd.ptwl0.mongodb.net/bigdataproject --conf spark.mongodb.connection.uri=mongodb+srv://remote_worker:remote_worker@bddbd.ptwl0.mongodb.net/bigdataproject --conf spark.mongodb.database=bigdataproject --conf spark.mongodb.collection=twitter.tweet --packages org.mongodb.spark:mongo-spark-connector:10.0.0 --total-executor-cores 4 --executor-cores 2 --executor-memory 5g --driver-memory 5g --name datalake_to_mongo /home/bigdata/tweet_election_project/src/spark/formating.py

spark = SparkSession(SparkContext(conf=SparkConf()).getOrCreate())

def datalake_to_mongo(date):
    # Load raw
    tweets_raw = spark.read.json(f"/home/bigdata/datalake/raw/twitter/{date}/*.json")

    # Extract Extract .withColumn("created_at", to_timestamp('created_at_')) \
    tweets = tweets_raw \
        .select(col('data.*')) \
        .drop(col('attachments')) \
        .withColumnRenamed("created_at", "created_at_") \
        .withColumnRenamed("id", "_id") \
        .drop(col('created_at_')) \
        .drop(col('geo'))

    tweets.write \
        .format("mongodb") \
        .mode("append") \
        .option("database", "bigdataproject") \
        .option("collection", "twitter.tweet") \
        .save()

    tweets.show()


def main():
    # for i in range(6,12):
    #     if i < 10:
    #         i = f'0{i}'
    #     date = f'202206{i}'
    #     print(f"Handling {date}")
    #     datalake_to_mongo(date)
    # return
    datalake_to_mongo('20220606')


if __name__ == '__main__':
    t1 = datetime.datetime.now()
    print('Started at :', t1)
    main()
    t2 = datetime.datetime.now()
    dist = t2 - t1
    print(f'Finished at: {t2} | elapsed time {dist.seconds}s')
    spark.sparkContext._gateway.close()
    spark.stop()
    sys.exit(0)

#df = spark.read.format("mongodb").load()





