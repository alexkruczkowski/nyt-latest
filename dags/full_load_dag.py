from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
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

    op1 = PythonOperator(
        task_id='get_books_data',
        python_callable=load_books_data,
        dag=dag,
    )

    op2 = PythonOperator(
        task_id='get_movies_data',
        python_callable=load_movies_data,
        dag=dag,
    )

    op3 = PostgresOperator(
		task_id='initialize_target_db',
		postgres_conn_id='postgres_conn',
		sql='sql/init_db_schema.sql',
		dag=dag
	)


(op1, op2) >> op3