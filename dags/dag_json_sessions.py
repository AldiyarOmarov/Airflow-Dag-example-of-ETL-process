import os
import random
import logging
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor


# Определение функции для выравнивания вложенного JSON
def flatten_nested_json(df, column_name):
    """
    Flatten nested JSON data into a DataFrame..
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
        if file.startswith('ga_sessions') and file.endswith('.json'):
            json_file_path = os.path.join(input_folder, file)
            try:
                # Чтение JSON-файла в DataFrame
                df = pd.read_json(json_file_path)

                # Выравнивание вложенных данных JSON
                df = flatten_nested_json(df, df.columns[0])

                # Удаление столбца 'event_value', если он существует
                df = pd.DataFrame(df)
                df.loc[df['device_brand'] == 'Apple', 'device_os'] = df.loc[df['device_brand'] == 'Apple', 'device_os'].fillna('iOS')
                df.loc[df['device_os'] == 'iOS', 'device_brand'] = df.loc[df['device_os'] == 'iOS', 'device_brand'].fillna('Apple')
                df.loc[(df['device_os'] == 'iOS') & (df['device_brand'] == '(not set)'), 'device_brand'] = 'Apple'
                android_brands = df.loc[df['device_os'] == 'Android', 'device_brand'].tolist()
                android_brands = [brand for brand in android_brands if brand != '(not set)']
                android_brands = list(set(android_brands))

                value_to_replace_2 = '(not set)'
                android_brands = [brand for brand in android_brands if brand != value_to_replace_2]

                df.loc[df['device_os'] == 'Android', 'device_brand'] = df.loc[df['device_os'] == 'Android', 'device_brand'].replace(value_to_replace_2, random.choice(android_brands))
                df.loc[df['device_brand'].isin(android_brands), 'device_os'] = 'Android'

                # Обновление 'device_brand' в соответствии с условиями
                df.loc[(df['device_os'] == 'Windows') & (df['device_brand'].isna()) & (df['device_category'] == 'mobile'), 'device_brand'] = 'Nokia'
                df.loc[(df['device_os'] == 'Windows') & (df['device_brand'].isna()) & (df['device_category'] == 'desktop'), 'device_brand'] = 'PC windows'
                df.loc[(df['device_os'] == 'Linux') & (df['device_brand'].isna()) & (df['device_category'] == 'desktop'), 'device_brand'] = 'PC linux'

                # Удаление конкретных строк и столбцов
                df = df.loc[~((df['device_brand'] == '(not set)') | (df['device_os'] == '(not set)'))]
                df = df.dropna(subset=['device_os', 'device_brand'], how='any')
                df = df.drop('device_model', axis=1)
                df['utm_medium'] = df['utm_medium'].replace('(none)', pd.NA)
                df_cleaned = df.dropna(subset=['utm_keyword', 'utm_adcontent', 'utm_campaign', 'utm_source', 'utm_medium'], how='any')

                # Загрузка очищенных данных в базу данных PostgreSQL
                try:
                    # Вставка данных в таблицу
                    df_cleaned.to_sql(name='ga_sessions', con=engine, if_exists='append', index=False)
                    # Журналирование успешной загрузки
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
    'json_sessions_etl',
    default_args=default_args,
    description='ETL of JSON Sessions',
    schedule_interval='@daily',
)

# Определение ExternalTaskSensor для ожидания завершения DAG 'ga_sessions_etl'
wait_for_ga_sessions_etl = ExternalTaskSensor(
    task_id='wait_for_ga_sessions_etl',
    external_dag_id='ga_sessions_etl',
    external_task_id='ingest_sessions_to_database',
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
wait_for_ga_sessions_etl >> process_json_operator
