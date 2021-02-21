from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from spotify_etl import run_spotify_etl

default_args = {
    'owner':'airflow',
    'depends_on_past' : False,
    'start_date' : days_ago(0, 0, 0, 0, 0),
    'email': ["mustafa.aykon@gmail.com"],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay': timedelta(minutes=1)

}

dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description= 'First DAG with ETL Process',
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id = "whole_spotify_etl",
    python_callable= run_spotify_etl,
    dag=dag
)

email_task = EmailOperator(
    task_id = "email_operator",
    to = "mustafa.aykon@gmail.com",
    subject = "Automated Report",
    html_content = "<p> This is automatic email about spotify API <p>",
    files = ["played_tracks.sqlite"],
    dag=dag
 )

run_etl >> email_task