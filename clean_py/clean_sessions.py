#!/usr/bin/env python3
import os
import pandas as pd
import uuid
import random

def load_and_clean_sessions(file_path):
    # Загружаю и очищаю набор данных 'ga_sessions'
    df = pd.read_csv(file_path, low_memory=False)
    # Очистка и обработка данных
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

    # Обновляю 'device_brand' в соответствии с условиями
    df.loc[(df['device_os'] == 'Windows') & (df['device_brand'].isna()) & (df['device_category'] == 'mobile'), 'device_brand'] = 'Nokia'
    df.loc[(df['device_os'] == 'Windows') & (df['device_brand'].isna()) & (df['device_category'] == 'desktop'), 'device_brand'] = 'PC windows'
    df.loc[(df['device_os'] == 'Linux') & (df['device_brand'].isna()) & (df['device_category'] == 'desktop'), 'device_brand'] = 'PC linux'

    # Удаляю определенные строки и столбцы
    df = df.loc[~((df['device_brand'] == '(not set)') | (df['device_os'] == '(not set)'))]
    df = df.dropna(subset=['device_os', 'device_brand'], how='any')
    df = df.drop('device_model', axis=1)
    df['utm_medium'] = df['utm_medium'].replace('(none)', pd.NA)
    df = df.dropna(subset=['utm_keyword', 'utm_adcontent', 'utm_campaign', 'utm_source', 'utm_medium'], how='any')
    return df

def save_cleaned_data(df, output_folder, output_file):
    # Сохраняю очищенные данные в указанную папку
    output_path = os.path.join(output_folder, output_file)
    df.to_csv(output_path, index=False)

def convert_uuid_to_float(uuid_str):
    # Преобразовываю UUID-строку в число с плавающей точкой
    if pd.notna(uuid_str):
        if isinstance(uuid_str, (int, float)):
            return float(uuid_str)
        elif '.' in str(uuid_str):
            try:
                return float(uuid_str)
            except ValueError:
                return None
        else:
            try:
                uuid_obj = uuid.UUID(uuid_str)
                return float(uuid_obj.int)
            except ValueError:
                return None
    else:
        return None

def main():
    # Основная функция для выполнения процесса ETL.
    print("Текущий рабочий каталог:", os.getcwd())

    # Задаю папки для сохранения очищенных данных
    output_sessions_folder = 'main_dataset/cleaned_datasets'

    # Загружаю и очищаю набор данных 'ga_sessions.csv'
    input_sessions_file = 'main_dataset/ga_sessions.csv'
    cleaned_sessions_file = 'ga_sessions_cleaned.csv'
    df_sessions_cleaned = load_and_clean_sessions(input_sessions_file)
    save_cleaned_data(df_sessions_cleaned, output_sessions_folder, cleaned_sessions_file)

if __name__ == "__main__":
    main()
