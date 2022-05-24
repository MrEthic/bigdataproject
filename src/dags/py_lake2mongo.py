import airflow
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'admin',
    'start_date': datetime(2020, 11, 18),
    'retries': 3,
	  'retry_delay': timedelta(hours=1)
}
with airflow.DAG('test_py',
                  default_args=default_args,
                  schedule_interval='0 1 * * *') as dag:
    task_elt_documento_pagar = BashOperator(
        task_id='test_py_user',
        bash_command="/home/bigdata/tweet_election_project/venv/bin/python /home/bigdata/tweet_election_project/src/spark/extract_tweet_users.py",
    )