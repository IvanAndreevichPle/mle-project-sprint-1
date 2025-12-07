import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from steps.churn import create_table, extract, transform, load
from steps.messages import send_telegram_success_message, send_telegram_failure_message

with DAG(
    dag_id='alt_churn',
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message,
) as dag:
    
    # Задача создания таблицы
    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
    )
    
    # Задача извлечения данных
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )
    
    # Задача преобразования данных
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )
    
    # Задача загрузки данных
    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )
    
    # Определяем порядок выполнения задач
    create_table_task >> extract_task >> transform_task >> load_task

