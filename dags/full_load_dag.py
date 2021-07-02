from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago

from books_etl import load_books_data
from movies_etl import load_movies_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0,0,0,0,0),
    'email': ['airflow@sample.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG('nyt_full_load',
		default_args=default_args,
		description='Executes full load from NYT bestsellers and movie review APIs to Production DW.',
		max_active_runs=1,
		schedule_interval=None) as dag:

    load_bestsellers_s3 = PythonOperator(
        task_id='books_etl',
        python_callable=load_books_data,
        dag=dag,
    )

    load_movies_s3 = PythonOperator(
        task_id='movies_etl',
        python_callable=load_movies_data,
        dag=dag,
    )

load_bestsellers_s3 >> load_movies_s3