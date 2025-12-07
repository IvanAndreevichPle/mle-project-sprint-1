# Проект спринта 1: Airflow + DVC для оценки стоимости квартир

## Структура репозитория

- `part1_airlfow/` — пайплайны Airflow:
  - `dags/` — DAG'и:
    - `churn.py`, `clean_churn.py`, `alt_churn.py`, `prepare_churn_dataset.py`, `flats_features.py` — DAG'и для ETL.
    - Основные DAG'и проекта по недвижимости:
      - `flats_features.py` — сбор витрины признаков квартир из таблиц `buildings` и `flats` в БД.
  - `logs/` — логи выполнения DAG'ов Airflow.
  - `notebooks/` — ноутбуки с анализом и подготовкой данных.
  - `plugins/` — плагины Airflow:
    - `steps/flats.py` — шаги DAG'а по сборке витрины `flats_features`.
    - `steps/messages.py` — Telegram-уведомления об успехе/ошибке.
  - корень `part1_airlfow/`:
    - `docker-compose.yaml`, `Dockerfile`, `requirements.txt`, `config/` — окружение для запуска Airflow.

- `part2_dvc/` — DVC-пайплайн обучения модели:
  - `scripts/`:
    - `data.py` — выгрузка данных из БД и сохранение в `data/initial_data.csv`.
    - `fit.py` — обучение модели (sklearn-пайплайн с OneHotEncoder + LogisticRegression).
    - `evaluate.py` — кросс-валидация и сохранение метрик.
  - `data/` — входные данные для обучения (`initial_data.csv`).
  - `models/` — обученные модели (`fitted_model.pkl`).
  - `cv_results/` — результаты кросс-валидации (`cv_res.json`).
  - файлы конфигурации DVC:
    - `dvc.yaml` — описание стадий `get_data`, `fit_model`, `evaluate_model`.
    - `params.yaml` — гиперпараметры (в т.ч. блока `logreg`).
    - `dvc.lock` — зафиксированное состояние пайплайна.

## Имя S3-бакета

Обученная модель и артефакты DVC хранятся в бакете:

`s3-student-mle-20251116-8842c990e3-freetrack`


