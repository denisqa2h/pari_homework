# SETUP

Разработка шла на WSL2, Ubuntu

Steps:

1. Поднять докер:
   
<pre>
sudo docker-compose -f ./airflow-docker-compose.yaml --env-file ./.env up -d --build
</pre>
2. Создать базу:

<pre>
docker exec -it <имя контейнера postgres> psql -U airflow -h postgres -c "CREATE DATABASE pari_football;"
</pre>

Адрес airflow:

<pre>
localhost:8080
</pre>

# Модули

database.py - Класс с методами использование psycopg2 функций

get_data_in_psql_dag.py - Скрипт обращается к API, льет полученные данные в сыром виде в PSQL

make_fixtures_pl_stats_dag.py - Собирает данные из источников формирует табличку для отчетности

make_pdf_report_pl_stats_dag.py - Формирует отчет в PDF

orchestration_dag.py - Оркестрация скриптов и формирование DAG'a


Сформированные отчеты в dags/docs/report_<время сформированного отчета>
