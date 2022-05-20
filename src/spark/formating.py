from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
import datetime
import sys


spark = SparkSession(SparkContext(conf=SparkConf()).getOrCreate())

def datalake_to_mongo():
    # Load raw
    tweets_raw = spark.read.json("/home/bigdata/datalake/raw/twitter/20220501/*.json")

    # Extract Extract
    users = tweets_raw.select(col('includes.users'))\
        .select(explode("users"))\
        .select(col('col.*'))\
        .withColumnRenamed("id","_id")\
        .dropDuplicates(["_id"])\
        .drop(col('entities'))


    print(users.show())

    users.write\
        .format("mongodb")\
        .mode("append")\
        .option("database","bigdataproject")\
        .option("collection", "twitter.user")\
        .save()


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
    spark.stop()
    sys.exit(0)


#df = spark.read.format("mongodb").load()





