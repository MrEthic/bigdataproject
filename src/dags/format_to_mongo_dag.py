from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

local_tz = pendulum.timezone("Europe/Paris")
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 20, tzinfo=local_tz),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

pyspark_app_home = Variable.get("BIGDATA_SPARK_HOME")

with DAG('format_to_mongo_dag',
         default_args=default_args,
         schedule_interval='0 1 * * *') as dag:

    datalake_to_mongo = SparkSubmitOperator(
        task_id='datalake_to_mongo',
        conn_id='spark_standalone_cm',
        application=f'{pyspark_app_home}/formating.py',
        total_executor_cores=4,
        packages="org.mongodb.spark:mongo-spark-connector:10.0.1",
        executor_cores=2,
        executor_memory='5g',
        driver_memory='5g',
        name='datalake_to_mongo',
        execution_timeout=timedelta(minutes=10),
        conf={
            "spark.executorEnv.JAVA_HOME": "/usr/lib/jvm/java-11-openjdk-amd64",
            "spark.yarn.appMasterEnv.JAVA_HOME": "/usr/lib/jvm/java-11-openjdk-amd64",
            "spark.mongodb.input.uri": "mongodb+srv://remote_worker:remote_worker@bddbd.ptwl0.mongodb.net/",
            "spark.mongodb.output.uri": "mongodb+srv://remote_worker:remote_worker@bddbd.ptwl0.mongodb.net/",
            "spark.mongodb.connection.uri": "mongodb+srv://remote_worker:remote_worker@bddbd.ptwl0.mongodb.net/",
        }
    )
