import os
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine


def main():
    # Загружаем переменные окружения из .env
    load_dotenv(find_dotenv())

    # Берём креды именно к той БД, куда Airflow складывает таблицу users_churn
    dst_host = os.environ.get('DB_DESTINATION_HOST')
    dst_port = os.environ.get('DB_DESTINATION_PORT')
    dst_username = os.environ.get('DB_DESTINATION_USER')
    dst_password = os.environ.get('DB_DESTINATION_PASSWORD')
    dst_db = os.environ.get('DB_DESTINATION_NAME')

    # Создаем соединение с БД назначения (где лежит users_churn)
    engine = create_engine(
        f'postgresql://{dst_username}:{dst_password}@{dst_host}:{dst_port}/{dst_db}'
    )

    # Здесь можно менять SQL под нужную вам выборку
    TABLE = 'users_churn'
    sql = f'select * from {TABLE}'
    data = pd.read_sql(sql, engine)

    os.makedirs('data', exist_ok=True)
    data.to_csv('data/initial_data.csv', index=False)


if __name__ == '__main__':
    main()


