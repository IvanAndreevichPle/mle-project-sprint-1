# scripts/fit.py

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.linear_model import LogisticRegression
import yaml
import os
import joblib

# обучение модели
def fit_model():
    # Прочитайте файл с гиперпараметрами params.yaml
    with open('params.yaml', 'r') as f:
        params = yaml.safe_load(f)
    
    # загрузите результат предыдущего шага: initial_data.csv
    data = pd.read_csv('data/initial_data.csv', index_col=params['index_col'])
    
    # реализуйте основную логику шага с использованием гиперпараметров
    
    # Определение признаков
    cat_features = data.select_dtypes(include='object')
    num_features = data.select_dtypes(['float'])

    # Простая обработка пропусков:
    # - числовые признаки: заполняем медианой
    # - категориальные: заполняем модой
    for col in num_features.columns:
        data[col] = data[col].fillna(data[col].median())
    for col in cat_features.columns:
        data[col] = data[col].fillna(data[col].mode().iloc[0])

    # Создание preprocessor: ко всем категориальным признакам OneHotEncoder
    preprocessor = ColumnTransformer(
        [
            (
                'cat',
                OneHotEncoder(
                    drop=params['one_hot_drop'],
                    handle_unknown='ignore'
                ),
                cat_features.columns.tolist()
            ),
            (
                'num',
                StandardScaler(),
                num_features.columns.tolist()
            ),
        ],
        remainder='drop',
        verbose_feature_names_out=False,
    )

    # Создание модели LogisticRegression
    logreg_params = params.get('logreg', {})
    model = LogisticRegression(
        C=logreg_params.get('C', 1.0),
        penalty=logreg_params.get('penalty', 'l2'),
        max_iter=logreg_params.get('max_iter', 1000),
    )
    
    # Создание и обучение pipeline
    pipeline = Pipeline(
        steps=[
            ('preprocessor', preprocessor),
            ('model', model)
        ]
    )
    
    X_train = data.drop(columns=[params['target_col']])
    y_train = data[params['target_col']]
    
    pipeline.fit(X_train, y_train)
    
    # сохраните обученную модель в models/fitted_model.pkl
    os.makedirs('models', exist_ok=True)
    joblib.dump(pipeline, 'models/fitted_model.pkl')

if __name__ == '__main__':
    fit_model()
