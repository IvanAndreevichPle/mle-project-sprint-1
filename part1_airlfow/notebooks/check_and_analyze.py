import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Подгружаем .env (указываем явный путь)
load_dotenv('.env')

# Считываем креды для destination_db
dst_host = os.environ.get('DB_DESTINATION_HOST')
dst_port = os.environ.get('DB_DESTINATION_PORT')
dst_username = os.environ.get('DB_DESTINATION_USER')
dst_password = os.environ.get('DB_DESTINATION_PASSWORD')
dst_db = os.environ.get('DB_DESTINATION_NAME')

print(f"Подключаюсь к базе: {dst_db}")
print(f"Хост: {dst_host}")

# Создаем соединение
conn = create_engine(f'postgresql://{dst_username}:{dst_password}@{dst_host}:{dst_port}/{dst_db}')

# Проверяем, существует ли таблица
try:
    result = pd.read_sql("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'users_churn')", conn)
    if result.iloc[0, 0]:
        print("✓ Таблица users_churn существует")
        
        # Загружаем ВСЕ данные
        data = pd.read_sql('SELECT * FROM users_churn', conn)
        
        print(f'\ndata shape = {data.shape}')
        print(data.head())
        
        # Создаем features, исключая customer_id и target
        features = data.drop(columns=['customer_id', 'target'], errors='ignore')
        
        # Выводим типы данных
        print("\n" + "="*50)
        print("Типы данных в features:")
        print("="*50)
        print(features.dtypes)
        
        # Количество признаков с каждым типом
        print("\n" + "="*50)
        print("Количество признаков с каждым типом:")
        print("="*50)
        print(features.dtypes.value_counts())
        
        # Детальная информация
        print("\n" + "="*50)
        print("Детальная информация о типах данных:")
        print("="*50)
        for dtype in sorted(features.dtypes.unique()):
            count = (features.dtypes == dtype).sum()
            columns = features.select_dtypes(include=[dtype]).columns.tolist()
            print(f"\n{dtype}: {count} признак(ов)")
            print(f"  Колонки: {', '.join(columns)}")
    else:
        print("✗ Таблица users_churn не существует в базе данных")
except Exception as e:
    print(f"Ошибка: {e}")
    import traceback
    traceback.print_exc()

conn.dispose()


