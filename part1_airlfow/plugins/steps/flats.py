import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    Float,
    String,
    Boolean,
)


def create_table(**kwargs):
    """Создаёт витринную таблицу flats_features в БД назначения."""
    hook = PostgresHook('destination_db')
    engine = hook.get_sqlalchemy_engine()

    metadata = MetaData()

    flats_features = Table(
        'flats_features',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=False),
        Column('building_id', Integer),
        Column('floor', Integer),
        Column('kitchen_area', Float),
        Column('living_area', Float),
        Column('rooms', Integer),
        Column('is_apartment', Boolean),
        Column('studio', Boolean),
        Column('total_area', Float),
        Column('price', Float),
        Column('build_year', Integer),
        Column('building_type_int', Integer),
        Column('latitude', Float),
        Column('longitude', Float),
        Column('ceiling_height', Float),
        Column('flats_count', Integer),
        Column('floors_total', Integer),
        Column('has_elevator', Boolean),
    )

    metadata.create_all(engine)
    engine.dispose()


def extract(**kwargs):
    """
    #### Extract
    Загружает объединённые данные из таблиц buildings и flats
    из персональной БД (destination_db).
    """
    hook = PostgresHook('destination_db')
    conn = hook.get_conn()

    sql = """
    SELECT
        f.id,
        f.building_id,
        f.floor,
        f.kitchen_area,
        f.living_area,
        f.rooms,
        f.is_apartment,
        f.studio,
        f.total_area,
        f.price,
        b.build_year,
        b.building_type_int,
        b.latitude,
        b.longitude,
        b.ceiling_height,
        b.flats_count,
        b.floors_total,
        b.has_elevator
    FROM flats AS f
    LEFT JOIN buildings AS b
        ON f.building_id = b.id;
    """

    data = pd.read_sql(sql, conn)
    conn.close()

    ti = kwargs["ti"]
    ti.xcom_push(key="extracted_flats", value=data)
    return data


def transform(**kwargs):
    """
    #### Transform
    Базовая трансформация (пока без сложной очистки):
    просто возвращаем датафрейм как есть.
    Позже сюда можно добавить очистку дубликатов, пропусков и выбросов.
    """
    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids="extract", key="extracted_flats")

    if data is None:
        raise ValueError("Не удалось получить данные из extract через XCom.")

    if not isinstance(data, pd.DataFrame):
        raise TypeError(f"Ожидался pandas.DataFrame, получен {type(data)}")

    # Здесь можно будет добавить первичную проверку/преобразование
    ti.xcom_push(key="transformed_flats", value=data)
    return data


def load(**kwargs):
    """
    #### Load
    Загружает витринный датафрейм в таблицу flats_features в БД назначения.
    """
    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids="transform", key="transformed_flats")

    if data is None:
        raise ValueError("Не удалось получить данные из transform через XCom.")

    if not isinstance(data, pd.DataFrame):
        raise TypeError(f"Ожидался pandas.DataFrame, получен {type(data)}")

    hook = PostgresHook('destination_db')
    hook.insert_rows(
        table="flats_features",
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=['id'],
        rows=data.values.tolist(),
    )


