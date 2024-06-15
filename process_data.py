from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'process_data',
    default_args=default_args,
    schedule_interval='@daily',
)

process_data_task = SparkSubmitOperator(
    task_id='process_data',
    application='file:///home/tugas/airflow/dags/ingest_data.py',
    conn_id='sparkwahid',
    conf={"spark.jars": "file:///home/tugas/postgresql-42.7.3.jar"},
    dag=dag,
)
