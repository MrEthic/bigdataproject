from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
import datetime

# Create SparkSession
# master('local[1]').\
#    config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector:10.0.0').\
spark = SparkSession.builder.\
    config("spark.mongodb.input.uri", "mongodb+srv://remote_worker:remote_worker@bddbd.ptwl0.mongodb.net/bigdataproject").\
    config("spark.mongodb.output.uri", "mongodb+srv://remote_worker:remote_worker@bddbd.ptwl0.mongodb.net/bigdataproject").\
    config("spark.mongodb.connection.uri","mongodb+srv://remote_worker:remote_worker@bddbd.ptwl0.mongodb.net/bigdataproject").\
    config("spark.mongodb.database", "bigdataproject").\
    config("spark.mongodb.collection", "twitter.tweet").\
    getOrCreate()

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
    print('started at :', t1)
    main()
    t2 = datetime.datetime.now()
    dist = t2 - t1
    print('finished at:', t2, ' | elapsed time (s):', dist.seconds)


#df = spark.read.format("mongodb").load()





