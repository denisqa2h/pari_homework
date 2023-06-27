from database import PostgreSQLConnection
from fpdf import FPDF
from datetime import timedelta, datetime, timezone
import os
from functools import wraps

connection = PostgreSQLConnection()
connection.connect()

#Логер функций
def logger(func):

    @wraps(func)
    def inner(*args, **kwargs):
        called_at = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=3)))
        print(f">>> Running {func.__name__!r} function. Logged at {called_at}")
        to_execute = func(*args, **kwargs)
        print(f">>> Function: {func.__name__!r} executed. Logged at {called_at}")
        return to_execute

    return inner

#Собираем данные из табличек для отчета в PDF, преобразовываем в понятный формат для либы FPDF2
@logger
def make_table_data_for_report(table_name):
    connection.execute(f"SELECT * FROM {table_name} limit 0")
    column_names = [desc[0] for desc in connection.cursor.description]
    connection.execute(f"SELECT * FROM {table_name}")
    rows = connection.cursor.fetchall()
    column_names = list(map(str, column_names))
    column_names = [tuple(column_names)]
    for i in rows:
        i = list(map(str, i))
        column_names.append(tuple(i))
    combined_tuple = tuple(column_names)
    return combined_tuple

#Собирает сам отчет в PDF, лежать будет в */dags/docs/report_таймстемп даты формирования отчета
@logger
def make_report(input_data):
    directory_path = os.path.abspath(os.path.dirname(__file__))
    pdf = FPDF(orientation="landscape")
    pdf.add_page()
    pdf.add_font('NotoSans', 'B', directory_path + '/docs/NotoSans-Regular.ttf', '')
    pdf.set_font('NotoSans', 'B', 6)
    with pdf.table() as table:
        for data_row in input_data:
            row = table.row()
            for datum in data_row:
                row.cell(datum)
    current_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    filename = f'{directory_path}/docs/report_{current_timestamp}.pdf'
    pdf.output(filename)

make_report(make_table_data_for_report('pl_fixtures_stats'))

connection.close()
