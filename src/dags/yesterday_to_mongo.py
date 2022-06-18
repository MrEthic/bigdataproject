import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

import sys
sys.path.insert(0, '/home/bigdata/tweet_election_project/')

from src.spark import tweets_to_mongo, users_to_mongo, subject_agg


default_args = {
    'owner': 'admin',
    'start_date': datetime(2022, 1, 1),
    'retries': 0,
	  'retry_delay': timedelta(hours=1)
}
with airflow.DAG('yesterday_tweets_to_mongo', default_args=default_args, schedule_interval='0 1 * * *', catchup=False) as dag:
    tweets_to_mongo_dag = PythonOperator(
        task_id='elt_tweets_to_mongo',
        python_callable=tweets_to_mongo
    )

    users_to_mongo_dag = PythonOperator(
        task_id='etl_users_to_mongo',
        python_callable=users_to_mongo
    )

    agg_to_mongo_dag = PythonOperator(
        task_id='etl_count_words',
        python_callable=subject_agg
    )

    tweets_to_mongo_dag>>[users_to_mongo, agg_to_mongo_dag]
