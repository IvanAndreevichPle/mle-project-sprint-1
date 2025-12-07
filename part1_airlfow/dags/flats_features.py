import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from steps.flats import create_table, extract, transform, load
from steps.messages import (
    send_telegram_success_message,
    send_telegram_failure_message,
)


with DAG(
    dag_id="prepare_flats_features",
    schedule="@once",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL", "flats"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message,
) as dag:

    create_table_task = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
    )

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    create_table_task >> extract_task >> transform_task >> load_task


