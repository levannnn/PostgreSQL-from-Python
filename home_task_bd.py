import psycopg2
from pprint import pprint
import os
from dotenv import load_dotenv

load_dotenv()

def create_db(conn):
    # удаление таблиц
    cur.execute("""DROP TABLE IF EXISTS phones, clients;""")
    # создание таблиц
    cur.execute("""
        CREATE TABLE IF NOT EXISTS clients (
        id SERIAL PRIMARY KEY, first_name VARCHAR(30), 
        last_name VARCHAR(30), email VARCHAR(30) NOT NULL);
        """)
    conn.commit()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS phones (
        id SERIAL PRIMARY KEY, phone VARCHAR(30) NOT NULL,
        client_id INTEGER NOT NULL REFERENCES clients(id));
        """)
    conn.commit()


# Добавление клиентов
def add_client(conn, first_name, last_name, email, phone=None):
    cur.execute("""
        INSERT INTO clients(first_name, last_name, email) 
        VALUES(%s, %s, %s) RETURNING id, first_name, last_name;
        """, (first_name, last_name, email))
    new_client = cur.fetchone()
    print(f'Добавлен клиент {new_client}')
    if phone is not None:
        cur.execute("""
            INSERT INTO phones(client_id, phone) VALUES(%s, %s);
            """, (new_client[0], phone))
        conn.commit()


# Добавление телефонных номеров
def add_phone(conn, client_id, phone):
    search_phone = check_phone(cur, client_id, phone)
    if search_phone is None or len(search_phone) == 0:
        print(f'Добавлен телефон {phone} для клиента {client_id}')
        cur.execute("""
            INSERT INTO phones(client_id, phone) VALUES(%s, %s);
            """, (client_id, phone))
        conn.commit()


# Проверка наличия телефонного номера
def check_phone(cur, client_id, phone):
    cur.execute("""
        SELECT phone FROM phones WHERE client_id=%s AND phone=%s;
        """, (client_id, phone))
    search_phone = cur.fetchone()
    return search_phone


# Изменение данных о клиенте
def change_client(conn, client_id, first_name=None, last_name=None, email=None, phone=None):
    if first_name is not None:
        cur.execute("""
            UPDATE clients SET first_name=%s WHERE id=%s
            """, (first_name, client_id))
        print(f'Имя у клиента {client_id} заменено на {first_name}')
    if last_name is not None:
        cur.execute("""
            UPDATE clients SET last_name=%s WHERE id=%s
            """, (last_name, client_id))
        print(f'Фамилия у клиента {client_id} заменена на {last_name}')
    if email is not None:
        cur.execute("""
            UPDATE clients SET email=%s WHERE id=%s
            """, (email, client_id))
        print(f'Адрес у клиента {client_id} заменен на {email}')
    if phone is not None:
        add_phone(conn, client_id, phone)
        print(f'Телефон {phone} добавлен клиенту {client_id}')
    conn.commit()


# Удаление номера телефона
def delete_phone(conn, client_id, phone):
    cur.execute("""
        DELETE FROM phones WHERE client_id=%s and phone=%s;
        """, (client_id, phone))
    conn.commit()
    print(f'Номер {phone} у клиента {client_id} удален')


# Удаление клиента из таблицы
def delete_client(conn, client_id):
    cur.execute("""
        DELETE FROM phones WHERE client_id=%s;
        """, (client_id,))
    cur.execute("""
        DELETE FROM clients WHERE id=%s;
        """, (client_id,))
    conn.commit()
    print(f'Клиент {client_id} удален')


# Поиск клиента по его данным: имени, фамилии, email или телефону
def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    if phone is not None:
        cur.execute("""
            SELECT clients.id FROM clients
            JOIN phones ON phones.client_id = clients.id
            WHERE phones.phone=%s;
            """, (phone,))
    else:
        cur.execute("""
            SELECT id FROM clients 
            WHERE first_name=%s OR last_name=%s OR email=%s;
            """, (first_name, last_name, email))
    conn.commit()
    print('Клиент', end=' ')
    print(*cur.fetchone())





if __name__ == '__main__':
    

    with psycopg2.connect(database="home_bd_nt", user="postgres", password=os.getenv('PASSWORD')) as conn:
        with conn.cursor() as cur:
            # создание таблиц, удаление таблиц
            create_db(conn)

            # Добавление клиентов
            add_client(conn, 'Vanya', 'Ivanov', 'god@ya.ru', '5474578123')
            add_client(conn, 'Igor', 'Petrov', 'Igor@ya.ru')
            add_client(conn, 'Sasha', 'Sidorov', 'Sasha@ya.ru', '85312873')

            # Добавление телефонных номеров
            add_phone(conn, 3, '6347347347')
            add_phone(conn, 1, '3476347343')
            add_phone(conn, 2, '6646347342')

            # Изменение данных о клиенте
            change_client(conn, 1, 'Petya', None, None, '83125717')
            change_client(conn, 2, None, 'Sobakin')
            change_client(conn, 3, None, None, 'idorov@ya.ru')
            change_client(conn, 2, None, None, None, '87328515')

            # Удаление номера телефона
            delete_phone(conn, 1, '83125717')
            delete_phone(conn, 1, '82312352')

            # Удаление клиента из таблицы
            delete_client(conn, 3)

            # Поиск клиента по его данным: имени, фамилии, email или телефону
            find_client(conn, 'Igor')
            find_client(conn, None, 'Ivanov')
            find_client(conn, None, None, 'god@ya.ru')
            find_client(conn, None, None, None, '5474578123')
    conn.close()
