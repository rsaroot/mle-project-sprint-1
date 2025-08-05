from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum
from steps.churn import create_table, extract_data, load_data
from steps.messages import send_success_message, send_failure_message

with DAG(
    dag_id='part1_airflow',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule_interval='@once',
    catchup=False,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1,
    },
    tags=['project-1', 'etl'],
    on_success_callback=send_success_message,
    on_failure_callback=send_failure_message
) as dag:

    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
        op_kwargs={'table_name': 'flats_with_b_features'}
    )

    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        op_kwargs={'table_name': 'flats_with_b_features'}
    )

    create_table_task >> extract_data_task >> load_data_task