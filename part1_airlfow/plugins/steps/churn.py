import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import MetaData, Table, Column, String, Integer, Float, DateTime, UniqueConstraint


def create_table(**kwargs):
    """Создает таблицу alt_users_churn в базе данных"""
    hook = PostgresHook('destination_db')
    db_conn = hook.get_sqlalchemy_engine()
    
    # Создаем объект MetaData, который будет хранить информацию о таблице
    metadata = MetaData()
    
    # Определяем структуру таблицы
    alt_users_churn = Table(
        'alt_users_churn',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),  # Индекс id на первом месте
        Column('customer_id', String),
        Column('begin_date', DateTime),
        Column('end_date', DateTime),
        Column('type', String),
        Column('paperless_billing', String),
        Column('payment_method', String),
        Column('monthly_charges', Float),
        Column('total_charges', Float),
        Column('internet_service', String),
        Column('online_security', String),
        Column('online_backup', String),
        Column('device_protection', String),
        Column('tech_support', String),
        Column('streaming_tv', String),
        Column('streaming_movies', String),
        Column('gender', String),
        Column('senior_citizen', Integer),
        Column('partner', String),
        Column('dependents', String),
        Column('multiple_lines', String),
        Column('target', Integer),
        # Уникальное ограничение после всех колонок
        UniqueConstraint('customer_id', name='unique_customer_id_constraint_alt')
    )
    
    # Создаем таблицу в базе данных (create_all проверяет существование и создает, если таблицы нет)
    metadata.create_all(db_conn)
    db_conn.dispose()


def extract(**kwargs):
    """
    #### Extract task
    """
    hook = PostgresHook('source_db')
    conn = hook.get_conn()
    sql = """
    select
        c.customer_id, c.begin_date, c.end_date, c.type, c.paperless_billing, c.payment_method, c.monthly_charges, c.total_charges,
        i.internet_service, i.online_security, i.online_backup, i.device_protection, i.tech_support, i.streaming_tv, i.streaming_movies,
        p.gender, p.senior_citizen, p.partner, p.dependents,
        ph.multiple_lines
    from contracts as c
    left join internet as i on i.customer_id = c.customer_id
    left join personal as p on p.customer_id = c.customer_id
    left join phone as ph on ph.customer_id = c.customer_id
    """
    # Получаем данные
    data = pd.read_sql(sql, conn)
    conn.close()
    
    # Работаем с XCom через task_instance
    ti = kwargs['ti']
    ti.xcom_push(key='extracted_data', value=data)
    
    return data


def transform(**kwargs):
    """
    #### Transform task
    """
    ti = kwargs['ti']  # получение объекта task_instance
    data = ti.xcom_pull(task_ids='extract', key='extracted_data')  # выгрузка данных из task_instance
    
    # Проверяем, что данные получены
    if data is None:
        raise ValueError("Данные из задачи extract не найдены в XCom")
    
    # Проверяем, что это DataFrame
    if not isinstance(data, pd.DataFrame):
        raise TypeError(f"Ожидался DataFrame, получен {type(data)}")
    
    data['target'] = (data['end_date'] != 'No').astype(int)  # логика функции
    data['end_date'].replace({'No': None}, inplace=True)
    
    ti.xcom_push(key='transformed_data', value=data)  # вместо return отправляем данные передатчику task_instance
    
    return data


def load(**kwargs):
    """
    #### Load task
    """
    # Работаем с XCom через task_instance
    ti = kwargs['ti']
    
    # Забираем из transform
    data = ti.xcom_pull(task_ids='transform', key='transformed_data')
    
    # Проверяем, что данные получены
    if data is None:
        raise ValueError("Данные из задачи transform не найдены в XCom")
    
    # Проверяем, что это DataFrame
    if not isinstance(data, pd.DataFrame):
        raise TypeError(f"Ожидался DataFrame, получен {type(data)}")
    
    hook = PostgresHook('destination_db')
    hook.insert_rows(
        table="alt_users_churn",
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=['customer_id'],
        rows=data.values.tolist()
    )

