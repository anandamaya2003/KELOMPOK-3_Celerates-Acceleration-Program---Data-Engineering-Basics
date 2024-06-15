from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'extract_data',
    default_args=default_args,
    schedule_interval='@daily',
)

def extract_sql_report():
    postgres_hook = PostgresHook(postgres_conn_id='postgreswahidconn')
    sql = "SELECT order_id, customer_id, product_id, order_date_new, order_time, quantity, payment_mode FROM sql_report"
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()

    with open('/home/tugas/sql_report.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([desc[0] for desc in cursor.description])  
        writer.writerows(rows)
    cursor.close()
    conn.close()

def extract_sql_product():
    postgres_hook = PostgresHook(postgres_conn_id='postgreswahidconn')
    sql = "SELECT product_id, product_name, product_category, price FROM sql_product"
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()

    with open('/home/tugas/sql_product.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([desc[0] for desc in cursor.description])  
        writer.writerows(rows)
    cursor.close()
    conn.close()

extract_sql_report_task = PythonOperator(
    task_id='extract_sql_report',
    python_callable=extract_sql_report,
    dag=dag,
)

extract_sql_product_task = PythonOperator(
    task_id='extract_sql_product',
    python_callable=extract_sql_product,
    dag=dag,
)

extract_sql_report_task >> extract_sql_product_task
