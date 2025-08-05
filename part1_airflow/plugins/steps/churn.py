from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from sqlalchemy import (Table, Column, Integer,
                        Float, String, Boolean, DateTime,
                        MetaData, UniqueConstraint, ForeignKey)
from sqlalchemy import inspect

def create_table(**kwargs):
    table_name = kwargs.get('table_name', 'flats_with_b_features')
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
            Column('building_id', Integer),
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
    SELECT 
        f.id AS flat_id,
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
    FROM flats f
    JOIN buildings b ON f.building_id = b.id
    """
    
    data = pd.read_sql(sql, conn)
    conn.close()
    
    ti.xcom_push(key='extracted_data', value=data)

def load_data(**kwargs):
    ti = kwargs['ti']
    table_name = kwargs.get('table_name', 'flats_with_b_features')
    data = ti.xcom_pull(task_ids='extract_data', key='extracted_data')
    hook = PostgresHook('destination_db')
    hook.insert_rows(
        table=table_name,
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=['flat_id'],
        rows=data.values.tolist(),
        commit_every=1000
    )