from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators.s3_to_postgres_operator import S3ToPostgresOperator
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

    op4 = S3ToPostgresOperator(
		task_id='load_books_data',
		s3_conn_id='my_S3_conn',
		s3_bucket='nyt-api-bucket',
		s3_prefix='uploads/NYT_bestseller_data',
		source_data_type='csv',
		header=True,
		postgres_conn_id='postgres_conn',
		schema='raw',
		table='bestsellers_data',
		get_latest=True,
		dag=dag
	)

    op5 = S3ToPostgresOperator(
		task_id='load_movies_data',
		s3_conn_id='my_S3_conn',
		s3_bucket='nyt-api-bucket',
		s3_prefix='uploads/NYT_movie_review_data',
		source_data_type='csv',
		header=True,
		postgres_conn_id='postgres_conn',
		schema='raw',
		table='movies_data',
		get_latest=True,
		dag=dag
	)

    op6 = PostgresOperator(
		task_id='execute_full_load',
		postgres_conn_id='postgres_conn',
		sql='sql/full_load.sql',
		dag=dag
	)


(op1, op2) >> op3 >> (op4, op5) >> op6