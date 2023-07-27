from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Import the main function from Spotify_ETL module
from modules.Spotify_ETL import main as run_etl

# Define default arguments for the DAG
default_args= {
    'owner':'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0,0,0,0),
    'email': ['johnandreicm@gmail.com'],
    'email_on_failure':False,
    'email on retry': False,
    'retries':1,
    'retry_delay': timedelta(minutes=1)
    }

# Define the DAG instance with its attributes
dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description='Spotify_ETL DAG',
    schedule_interval='0 2 * * *', # Set to 10AM PH Time
)  

task1 = PythonOperator(
    task_id='SPOTIFY_ETL',
    python_callable=run_etl,
    dag=dag
)

task1
