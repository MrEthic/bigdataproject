import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

import sys
sys.path.insert(0, '/home/bigdata/tweet_election_project/')

from src.spark import datalake_to_mongo


default_args = {
    'owner': 'admin',
    'start_date': datetime(2022, 1, 1),
    'retries': 0,
	  'retry_delay': timedelta(hours=1)
}
with airflow.DAG('yesterday_tweets_to_mongo',
                  default_args=default_args, schedule_interval='0 1 * * *') as dag:
    yesterday_tweets_to_mongo = PythonOperator(
        task_id='elt_tweets_to_mongo',
        python_callable=datalake_to_mongo
    )
