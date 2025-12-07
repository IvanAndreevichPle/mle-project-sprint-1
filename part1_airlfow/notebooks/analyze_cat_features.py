import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv('.env')

dst_host = os.environ.get('DB_DESTINATION_HOST')
dst_port = os.environ.get('DB_DESTINATION_PORT')
dst_username = os.environ.get('DB_DESTINATION_USER')
dst_password = os.environ.get('DB_DESTINATION_PASSWORD')
dst_db = os.environ.get('DB_DESTINATION_NAME')

conn = create_engine(f'postgresql://{dst_username}:{dst_password}@{dst_host}:{dst_port}/{dst_db}')

# Загружаем данные
data = pd.read_sql('SELECT * FROM users_churn', conn)

# Создаем features, исключая customer_id и target
features = data.drop(columns=['customer_id', 'target'], errors='ignore')

# Создаем cat_features - только категориальные признаки (тип object)
cat_features = features.select_dtypes(include=['object'])

print("="*60)
print("УНИКАЛЬНЫЕ ЗНАЧЕНИЯ И ИХ ЧАСТОТЫ")
print("="*60)

# Для каждой категориальной переменной выводим уникальные значения и их частоты
for col in cat_features.columns:
    print(f"\n{col}:")
    print("-" * 40)
    # value_counts() показывает уникальные значения и их частоты
    value_counts = cat_features[col].value_counts(dropna=False)  # dropna=False чтобы показать NaN если есть
    for value, count in value_counts.items():
        # Заменяем NaN на строку для красивого вывода
        display_value = value if pd.notna(value) else 'NaN/None'
        print(f"  {display_value}: {count}")

conn.dispose()


