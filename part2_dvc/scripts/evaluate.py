# scripts/evaluate.py

import pandas as pd
from sklearn.model_selection import StratifiedKFold, cross_validate
import joblib
import json
import yaml
import os


def evaluate_model():
    # 1. Читаем гиперпараметры
    with open('params.yaml', 'r') as f:
        params = yaml.safe_load(f)

    target_col = params['target_col']

    # 2. Загружаем модель (через файловый дескриптор, как требуют условия)
    with open('models/fitted_model.pkl', 'rb') as fd:
        pipeline = joblib.load(fd)

    # 3. Загружаем данные аналогично обучению
    data = pd.read_csv('data/initial_data.csv')

    # Обработка пропусков так же, как в fit_model
    cat_features = data.select_dtypes(include='object')
    num_features = data.select_dtypes(['float'])
    for col in num_features.columns:
        data[col] = data[col].fillna(data[col].median())
    for col in cat_features.columns:
        data[col] = data[col].fillna(data[col].mode().iloc[0])

    # Матрица признаков и таргет
    X = data.drop(columns=[target_col])
    y = data[target_col]

    # 4. Кросс‑валидация (как в начальном примере)
    cv_strategy = StratifiedKFold(n_splits=5)
    cv_res = cross_validate(
        pipeline,
        X,
        y,
        cv=cv_strategy,
        n_jobs=-1,
        scoring=['f1', 'roc_auc'],
    )

    # Усредняем значения по фолдам
    for key, value in cv_res.items():
        cv_res[key] = round(value.mean(), 3)

    # 5. Сохраняем результат в cv_results/cv_res.json
    os.makedirs('cv_results', exist_ok=True)
    with open('cv_results/cv_res.json', 'w') as f:
        json.dump(cv_res, f, indent=4)


if __name__ == '__main__':
    evaluate_model()
