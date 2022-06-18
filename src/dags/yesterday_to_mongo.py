import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
#from src.spark import datalake_to_mongo
import sys

def test():
    print(sys.path)

default_args = {
    'owner': 'admin',
    'start_date': datetime(2022, 11, 6),
    'retries': 2,
	  'retry_delay': timedelta(hours=1)
}
with airflow.DAG('yesterday_tweets_to_mongo',
                  default_args=default_args) as dag:
    yesterday_tweets_to_mongo = PythonOperator(
        task_id='elt_tweets_to_mongo',
        python_callable=test
    )
