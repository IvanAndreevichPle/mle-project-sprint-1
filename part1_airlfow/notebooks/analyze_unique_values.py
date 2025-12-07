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

# Разделяем признаки по типам данных
num_features = features.select_dtypes(include=['float', 'int'])
date_features = features.select_dtypes(include='datetime64[ns]')
cat_features = features.select_dtypes(include='object')

# Получаем количество уникальных значений в каждой колонке с помощью nunique()
unique_counts = cat_features.nunique()

print(unique_counts)
print()

# Используем value_counts() на результатах nunique(), чтобы узнать,
# сколько колонок с разным количеством уникальных значений присутствует
distribution = unique_counts.value_counts().sort_index()

print(distribution)
print()

# Создаем binary_cat_features - только бинарные признаки (2 уникальных значения)
binary_cat_features = cat_features.loc[:, cat_features.nunique() == 2]

# Создаем other_cat_features - только небинарные категориальные признаки (!= 2)
other_cat_features = cat_features.loc[:, cat_features.nunique() != 2]

print("Бинарные категориальные признаки:")
print(binary_cat_features.head())
print()
print("Небинарные категориальные признаки:")
print(other_cat_features.head())
print()

# Разделяем binary_cat_features на yes_no_features и other_binary_features
# yes_no_features - признаки, где значения только Yes/No и пропуски
# other_binary_features - остальные бинарные признаки

yes_no_columns = []
other_binary_columns = []

for col in binary_cat_features.columns:
    # Получаем уникальные значения без NaN
    unique_values = set(binary_cat_features[col].dropna().unique())
    # Проверяем, состоят ли значения только из Yes и No
    if unique_values == {'Yes', 'No'} or unique_values == {'No', 'Yes'}:
        yes_no_columns.append(col)
    else:
        other_binary_columns.append(col)

yes_no_features = binary_cat_features[yes_no_columns]
other_binary_features = binary_cat_features[other_binary_columns]

print("Бинарные признаки с Yes/No:")
print(yes_no_features.head())
print()
print("Остальные бинарные признаки:")
print(other_binary_features.head())

conn.dispose()
