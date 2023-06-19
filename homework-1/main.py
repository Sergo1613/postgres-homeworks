"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import psycopg2

"""Создаем подключение к БД"""
conn = psycopg2.connect(host='localhost', dbname='north', user='postgres', password='1613')
try:
    with conn:
        """Создаем курсор"""
        with conn.cursor() as cur:
            # Внесение данных из файла CSV в таблицу "customers"
            with open('north_data/customers_data.csv', 'r') as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    cur.execute(
                        "INSERT INTO customers (customer_id, company_name, contact_name) VALUES (%s, %s, %s)",
                        row
                    )
            # Внесение данных из файла CSV в таблицу "employees"
            with open('north_data/employees_data.csv', 'r') as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    cur.execute(
                        "INSERT INTO employees (employee_id, first_name, last_name, title, birth_date, notes) VALUES (%s, %s, %s, %s, %s, %s)",
                        row
                    )
            # Внесение данных из файла CSV в таблицу "orders"
            with open('north_data/orders_data.csv', 'r') as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    cur.execute(
                        "INSERT INTO orders (order_id, customer_id, employee_id, order_date, ship_city) VALUES (%s, %s, %s, %s, %s)",
                        row
                    )

finally:
    conn.close()
