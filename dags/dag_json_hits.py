import os
import logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

# Определение функции для выравнивания вложенного JSON
def flatten_nested_json(df, column_name):
    """
    Выравнивание вложенных данных JSON в DataFrame.
    """
    nested_df = pd.json_normalize(df[column_name])
    flattened_df = pd.concat([df, nested_df], axis=1)
    flattened_df.drop(columns=[column_name], inplace=True)
    return flattened_df

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

# Определение функции для обработки JSON-файлов и загрузки их в базу данных
def process_json_files(input_folder, engine_url):
    engine = create_engine(engine_url)
    for file in os.listdir(input_folder):
        if file.startswith('ga_hits') and file.endswith('.json'):
            json_file_path = os.path.join(input_folder, file)
            try:
                # Чтение JSON-файла в DataFrame
                df = pd.read_json(json_file_path)
                
                # Выравнивание вложенных данных JSON
                df = flatten_nested_json(df, df.columns[0])

                # Удаление столбца 'event_value', если он существует
                df = df.drop('event_value', axis=1)
                    
                # Удаление строк с отсутствующими значениями
                df_cleaned = df.dropna()
                    
                # Загрузка очищенных данных в базу данных PostgreSQL
                try:
                    df_cleaned.to_sql(name='ga_hits', con=engine, if_exists='append', index=False)
                    logging.info(f"Successfully ingested data from file {file}")
                except IntegrityError as e:
                    logging.error(f"IntegrityError: {e}")
                    logging.error(f"Failed to insert data from {json_file_path} into the database.")
                # Обработка исключения, например, журналирование или пропуск файла
                else:
                    logging.warning(f"Failed to insert data from {json_file_path} into the database.")
            except Exception as e:
                    logging.error(f"An error occurred while processing the JSON file {json_file_path}: {e}")
                    logging.error(f"Error: {e}")


# Создание DAG
dag = DAG(
    'json_hits_etl',
    default_args=default_args,
    description='ETL обработка JSON хитов',
    schedule_interval='@daily', 
)

# Определение ExternalTaskSensor для ожидания завершения DAG 'ga_hits_etl'
wait_for_ga_hits_etl = ExternalTaskSensor(
    task_id='wait_for_ga_hits_etl',
    external_dag_id='ga_hits_etl',
    external_task_id='ingest_hits_to_database',
    mode='reschedule',
    timeout=3600, 
    poke_interval=60, 
    retries=0,  
    dag=dag,
)

# Определение задачи PythonOperator
process_json_operator = PythonOperator(
    task_id='process_json_and_ingest',
    python_callable=process_json_files,
    op_kwargs={'input_folder': 'extra_dataset',
               'engine_url': 'postgresql+psycopg2://airflow:airflow@postgres/airflow'},
    dag=dag,
)

# Установка зависимостей задач
wait_for_ga_hits_etl >> process_json_operator
