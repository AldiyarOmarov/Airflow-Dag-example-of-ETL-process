import pandas as pd
import logging
import os
from time import time
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Определение словаря default_args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Функция для создания структуры таблицы для ga_sessions
def create_table_structure(engine):
    with engine.connect() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS ga_sessions (
                session_id TEXT, 
                client_id TEXT, 
                visit_date TIMESTAMP WITHOUT TIME ZONE, 
                visit_time TEXT, 
                visit_number BIGINT, 
                utm_source TEXT, 
                utm_medium TEXT, 
                utm_campaign TEXT, 
                utm_adcontent TEXT, 
                utm_keyword TEXT, 
                device_category TEXT, 
                device_os TEXT, 
                device_brand TEXT, 
                device_screen_resolution TEXT, 
                device_browser TEXT, 
                geo_country TEXT, 
                geo_city TEXT
            );
        """)

# Функция для загрузки сессий в базу данных
def ingest_sessions_to_database():
    output_sessions_folder = 'main_dataset/cleaned_datasets'
    cleaned_sessions_file = 'ga_sessions_cleaned.csv'
    
    engine_url = 'postgresql+psycopg2://airflow:airflow@postgres/airflow'
    engine = create_engine(engine_url)
    engine.connect()

    # Создание структуры таблицы
    create_table_structure(engine)

    # Загрузка данных порциями
    chunksize = 100000
    df_iter = pd.read_csv(os.path.join(output_sessions_folder, cleaned_sessions_file), iterator=True, chunksize=chunksize, low_memory=False)

    # Загрузка данных порциями
    for df_chunk in df_iter:
        t_start = time()
        df_chunk['visit_date'] = pd.to_datetime(df_chunk['visit_date'])
        df_chunk['visit_time'] = pd.to_datetime(df_chunk['visit_time']).dt.time
        try:
            df_chunk.to_sql(name='ga_sessions', con=engine, if_exists='append', index=False)
        except IntegrityError as e:
            logging.error(f"IntegrityError: {e}")
            # Журналирование IntegrityError для идентификации конфликтующих записей
            logging.error(f"Conflict rows: {df_chunk[df_chunk.duplicated(subset='session_id')]}")
            continue
        t_end = time()
        logging.info(f'Inserted fragment of ga_sessions, took {t_end - t_start:.3f} seconds')

# Создание DAG
dag = DAG(
    'ga_sessions_etl',
    default_args=default_args,
    description='Sessions ETL DAG',
    schedule_interval='@daily', 
)

# Определение задач PythonOperator
create_table_operator = PythonOperator(
    task_id='create_table',
    python_callable=create_table_structure,
    op_kwargs={'engine': create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')},  # Передача engine в качестве аргумента
    dag=dag,
)

ingest_sessions_to_database_operator = PythonOperator(
    task_id='ingest_sessions_to_database',
    python_callable=ingest_sessions_to_database,
    dag=dag,
)

# Установка зависимостей задач
create_table_operator >> ingest_sessions_to_database_operator
