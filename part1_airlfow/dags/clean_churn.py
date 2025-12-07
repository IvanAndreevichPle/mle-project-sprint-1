import pendulum
from airflow.decorators import dag, task

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    tags=["ETL"]
)
def clean_churn_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    @task()
    def create_table():
        from sqlalchemy import Table, Column, DateTime, Float, Integer, Index, MetaData, String, UniqueConstraint, inspect
        
        hook = PostgresHook('destination_db')
        db_engine = hook.get_sqlalchemy_engine()

        metadata = MetaData()

        churn_table = Table('clean_users_churn', metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
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
            UniqueConstraint('customer_id', name='unique_clean_customer_constraint')
        )
        metadata.create_all(db_engine)
        db_engine.dispose()
    
    @task()
    def extract():
        """
        #### Extract task
        Выгружает все данные из таблицы users_churn
        """
        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = "SELECT * FROM users_churn"
        data = pd.read_sql(sql, conn).drop(columns=['id'])
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        """
        #### Transform task
        Трансформации по очистке данных:
        1. Удаление дубликатов
        2. Удаление выбросов
        3. Заполнение пропусков
        """
        # 1. Удаление дубликатов по customer_id
        data = data.drop_duplicates(subset=['customer_id'], keep='first')
        
        # 2. Удаление выбросов для числовых колонок
        numeric_cols = ['monthly_charges', 'total_charges']
        for col in numeric_cols:
            if col in data.columns:
                # Используем IQR метод для удаления выбросов
                Q1 = data[col].quantile(0.25)
                Q3 = data[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                # Удаляем выбросы
                data = data[(data[col] >= lower_bound) & (data[col] <= upper_bound)]
        
        # 3. Заполнение пропусков
        # Для числовых колонок - медиана
        for col in numeric_cols:
            if col in data.columns:
                data[col] = data[col].fillna(data[col].median())
        
        # Для категориальных колонок - мода (наиболее частое значение)
        categorical_cols = ['type', 'paperless_billing', 'payment_method', 
                          'internet_service', 'online_security', 'online_backup',
                          'device_protection', 'tech_support', 'streaming_tv',
                          'streaming_movies', 'gender', 'partner', 'dependents',
                          'multiple_lines']
        for col in categorical_cols:
            if col in data.columns:
                mode_value = data[col].mode()
                if len(mode_value) > 0:
                    data[col] = data[col].fillna(mode_value[0])
                else:
                    data[col] = data[col].fillna('Unknown')
        
        # Для senior_citizen - заполняем 0 (не является senior)
        if 'senior_citizen' in data.columns:
            data['senior_citizen'] = data['senior_citizen'].fillna(0)
        
        # Для target - заполняем 0
        if 'target' in data.columns:
            data['target'] = data['target'].fillna(0)
        
        return data

    @task()
    def load(data: pd.DataFrame):
        """
        #### Load task
        Загружает очищенные данные в таблицу clean_users_churn
        """
        hook = PostgresHook('destination_db')
        data['end_date'] = data['end_date'].astype('object').replace(np.nan, None)
        hook.insert_rows(
            table='clean_users_churn',
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['customer_id'],
            rows=data.values.tolist()
        )
    
    # Создаем задачи
    create_table_task = create_table()
    extract_task = extract()
    transform_task = transform(extract_task)
    load_task = load(transform_task)
    
    # Устанавливаем зависимости: create_table -> extract -> transform -> load
    create_table_task >> extract_task >> transform_task >> load_task

clean_churn_dataset()

