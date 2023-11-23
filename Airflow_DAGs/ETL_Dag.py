from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from ETL_Spotify import spotify_etl_func
from airflow.utils.dates import days_ago

my_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['afzalsolangi.ali@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
my_dag = DAG(
    'spotify_dag',
    default_args = my_args,
    description= 'Spotify ETL',
    schedule_interval= '50 6 * * *'
)


run_etl = PythonOperator(
    task_id='spotify_etl_postgresql',
    python_callable= spotify_etl_func,
    dag=my_dag
)
run_etl
