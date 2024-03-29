#!/usr/bin/env python3
import os
import pandas as pd

def load_and_clean_hits(file_path):
    # Загрузка и очистка набора данных 'ga_hits'
    df = pd.read_csv(file_path, low_memory=False)
    df = df.drop('event_value', axis=1)
    df = df.dropna()  
    return df

def save_cleaned_data(df, output_folder, output_file):
    # Сохранение очищенных данных в указанную папку
    output_path = os.path.join(output_folder, output_file)
    df.to_csv(output_path, index=False)

def main():
    # Основная функция для выполнения процесса ETL.
    print("Текущий рабочий каталог:", os.getcwd())

    # Задание папок для сохранения очищенных данных
    output_hits_folder = 'main_dataset/cleaned_datasets' 

    # Загрузка и очистка набора данных 'ga_hits.csv'
    input_hits_file = 'main_dataset/ga_hits.csv'
    cleaned_hits_file = 'ga_hits_cleaned.csv'
    df_hits_cleaned = load_and_clean_hits(input_hits_file)
    save_cleaned_data(df_hits_cleaned, output_hits_folder, cleaned_hits_file)

if __name__ == "__main__":
    main()
