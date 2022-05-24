from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date
import datetime
import sys

#spark-submit --master local[*] --conf spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 --conf spark.yarn.appMasterEnv.JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 --conf spark.mongodb.input.uri=mongodb+srv://remote_worker:remote_worker@bddbd.ptwl0.mongodb.net/bigdataproject --conf spark.mongodb.output.uri=mongodb+srv://remote_worker:remote_worker@bddbd.ptwl0.mongodb.net/bigdataproject --conf spark.mongodb.connection.uri=mongodb+srv://remote_worker:remote_worker@bddbd.ptwl0.mongodb.net/bigdataproject --conf spark.mongodb.database=bigdataproject --conf spark.mongodb.collection=twitter.tweet --packages org.mongodb.spark:mongo-spark-connector:10.0.0 --total-executor-cores 4 --executor-cores 2 --executor-memory 5g --driver-memory 5g --name datalake_to_mongo /home/bigdata/tweet_election_project/src/spark/formating.py

spark = SparkSession(SparkContext(conf=SparkConf()).getOrCreate())

def datalake_to_mongo():
    # Load raw
    tweets_raw = spark.read.json("/home/bigdata/datalake/raw/twitter/20220501/*.json")

    # Extract Extract
    tweets_raw = tweets_raw \
        .select(col('data.*'))\

    tweets_raw.show()

    return


def main():
    datalake_to_mongo()
    return


if __name__ == '__main__':
    t1 = datetime.datetime.now()
    print('Started at :', t1)
    main()
    t2 = datetime.datetime.now()
    dist = t2 - t1
    print(f'Finished at: {t2} | elapsed time {dist.seconds}s')
    #spark.sparkContext._gateway.close()
    #spark.stop()
    sys.exit(0)

#df = spark.read.format("mongodb").load()




