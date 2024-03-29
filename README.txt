1. Пароли
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    ports:
      - 5432:5432
  pgadmin:
    image: dpage/pgadmin4
    environment:
      PGADMIN_DEFAULT_EMAIL: 'pgadmin@example.com'
      PGADMIN_DEFAULT_PASSWORD: 'pgadmin_password'
      PGADMIN_LISTEN_PORT: 5050
      PGADMIN_DEFAULT_SERVER: 'airflow'
      PGADMIN_SERVER_JSON_FILE: /pgadmin4/servers.json (Добавил сервер, чтобы не вбивать вручную, требует пароль ДБ)

  webserver airflow
	login airflow
	pass airflow
	ports:
      	  - "8080:8080"
2. В Папке clean_py лежат питон скрипты и jupyter notebook по очистке датасетов
Чтобы сэкономить размер образа(image) я не загружал основной датасет в контейнер, а только его очищенную версию.

Так как гитхаб не пропустил файлы ga_sessions and ga_hits

нужно в корневой папке найти main_dataset

в нее положить ga_sessions ga_hits

После запустить 2 py файла в папке clean_py

Очистка датасета ga_sessions: заполнил пустоты и значения (not set) согласно моей логике, где не смог сопоставить данные, удалил
Очистка датасета ga_hits: удалил столбец event label и удалил строки с пустотами

Далее запустить контейнеры через командную строку
В корневой папке лежит Dockerfile и docker-compose

3.Содержание папки

user@DESKTOP-0QMUBHL MINGW64 ~/Dimplomka/airflow
$ ls__pycache__/  
config/  
docker-compose.yaml  
extra_dataset/  
main_dataset/  
requirements.txt  
servers.json
clean_py/     
dags/    
Dockerfile           
logs/           
plugins/       
scripts/
README

4. Рассположение папки
"C:\Users\user\Dimplomka\DS-DE--Final-Work"