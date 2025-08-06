from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from sqlalchemy import (Table, Column, Integer,
                        Float, String, Boolean, DateTime,
                        MetaData, UniqueConstraint)
from sqlalchemy import inspect

def create_table(**kwargs):
    table_name = kwargs.get('table_name', 'flats_with_b_features_cleab')
    hook = PostgresHook('destination_db')
    engine = hook.get_sqlalchemy_engine()
    metadata = MetaData()
    
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        Table(
            table_name,
            metadata,
            #flats
            Column('id', Integer, primary_key=True),
            Column('flat_id', Integer),
            Column('floor', Integer),
            Column('kitchen_area', Float),
            Column('living_area', Float),
            Column('rooms', Integer),
            Column('is_apartment', Boolean),
            Column('studio', Boolean),
            Column('total_area', Float),
            Column('price', Float),
            
            #building
            Column('build_year', Integer),
            Column('building_type_int', Integer),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('ceiling_height', Float),
            Column('flats_count', Integer),
            Column('floors_total', Integer),
            Column('has_elevator', Boolean),
            
            UniqueConstraint('flat_id', name=f'unique_flat_constraint_{table_name}'),
        )
        metadata.create_all(engine)
        print(f"Table {table_name} created successfully.")
    else:
        print(f"Table {table_name} already exists, skipping creation.")

def extract_data(**kwargs):
    ti = kwargs['ti']
    hook = PostgresHook('destination_db')
    conn = hook.get_conn()
    
    sql = """
    SELECT *
    FROM flats_with_b_features
    """
    
    data = pd.read_sql(sql, conn)
    conn.close()
    
    ti.xcom_push(key='extracted_data', value=data)

def transform_data(**kwargs):

    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids='extract_data', key='extracted_data')

    # Дубликаты
    feature_cols = data.columns.drop(['id', 'flat_id']).tolist()
    is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)
    data = data[~is_duplicated_features].reset_index(drop=True)

    # Выбросы
    num_cols = data[feature_cols].select_dtypes(['float', 'int']).columns
    threshold = 1.5
    potential_outliers = pd.DataFrame()

    for col in num_cols:
        quartile_1 = data[col].quantile(0.25)
        quartile_3 = data[col].quantile(0.75)
        iq_range = quartile_3 - quartile_1
        margin = threshold * iq_range
        lower = quartile_1 - margin
        upper = quartile_3 + margin
        potential_outliers[col] = ~data[col].between(lower, upper)

    outliers = potential_outliers.any(axis=1)
    data = data[~outliers]

    ti.xcom_push('transformed_data', data)

def load_data(**kwargs):
    ti = kwargs['ti']
    table_name = kwargs.get('table_name', 'flats_with_b_features_clean')
    data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
    hook = PostgresHook('destination_db')
    hook.insert_rows(
        table=table_name,
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=['flat_id'],
        rows=data.values.tolist(),
        commit_every=1000
    )