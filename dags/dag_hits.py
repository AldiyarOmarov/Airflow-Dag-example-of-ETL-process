import pandas as pd
import logging
import os
from datetime import datetime, timedelta
from time import time
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

# Функция для создания структуры таблицы для ga_hits
def create_table_structure(engine):
    # Установка соединения с базой данных
    with engine.connect() as con:
        # Создание таблицы 'ga_hits', если она не существует
        con.execute("""
            CREATE TABLE IF NOT EXISTS ga_hits (
                session_id TEXT, 
                hit_date TIMESTAMP WITHOUT TIME ZONE, 
                hit_time FLOAT, 
                hit_number BIGINT, 
                hit_type TEXT, 
                hit_referer TEXT, 
                hit_page_path TEXT, 
                event_category TEXT, 
                event_action TEXT, 
                event_label TEXT
            );
        """)

# Функция для загрузки данных о хитах в базу данных
def ingest_hits_to_database():
    # Определение папки с очищенными данными и имени файла
    output_hits_folder = 'main_dataset/cleaned_datasets'
    cleaned_hits_file = 'ga_hits_cleaned.csv'
    
    # URL для подключения к базе данных
    engine_url = 'postgresql+psycopg2://airflow:airflow@postgres/airflow'
    engine = create_engine(engine_url)
    engine.connect()

    # Создание структуры таблицы
    create_table_structure(engine)

    # Загрузка данных порциями
    chunksize = 10000
    df_iter = pd.read_csv(os.path.join(output_hits_folder, cleaned_hits_file), iterator=True, chunksize=chunksize, low_memory=False)

    # Загрузка данных порциями
    for df_chunk in df_iter:
        t_start = time()
        df_chunk['hit_date'] = pd.to_datetime(df_chunk['hit_date'])
        try:
            df_chunk.to_sql(name='ga_hits', con=engine, if_exists='append', index=False)
        except IntegrityError as e:
            logging.error(f"IntegrityError: {e}")
            # Журналирование IntegrityError
            pass
        t_end = time()
        logging.info(f'Inserted fragment of ga_hits, took {t_end - t_start:.3f} seconds')

# Создание DAG
dag = DAG(
    'ga_hits_etl',
    default_args=default_args,
    description='Hits ETL DAG',
    schedule_interval='@daily',
)

# Определение задач PythonOperator
create_table_operator = PythonOperator(
    task_id='create_table',
    python_callable=create_table_structure,
    op_kwargs={'engine': create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')},
    dag=dag,
)

ingest_hits_to_database_operator = PythonOperator(
    task_id='ingest_hits_to_database',
    python_callable=ingest_hits_to_database,
    dag=dag,
)

# Установка зависимостей задач
create_table_operator >> ingest_hits_to_database_operator
