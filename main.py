import psycopg2
from psycopg2.sql import SQL, Identifier

# функция, удаляющая структуру БД (таблицы) #

def drop_tables(conn):
    cur.execute("""
    DROP TABLE IF EXISTS phone_client;
    DROP TABLE IF EXISTS client;
    """)
# функция, создающая структуру БД (таблицы) #

def create_table(conn):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS client(
        client_id SERIAL PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(80) NOT NULL,
        email VARCHAR(70) NOT NULL UNIQUE
    );
    """)    

    cur.execute("""
    CREATE TABLE IF NOT EXISTS phone_client(
        phone_client_id SERIAL PRIMARY KEY,
        phone VARCHAR(50) UNIQUE,
        client_id INTEGER REFERENCES client(client_id)
    );
    """)
# функция, позволяющая добавить нового клиента #

def add_client(conn, first_name, last_name, email, phone=None):
    cur.execute("""
    INSERT INTO client(first_name, last_name, email) VALUES(%s, %s, %s)
    """, (first_name, last_name, email))
    if phone:
        cur.execute("""
        SELECT client_id FROM client
        WHERE first_name=%s AND last_name=%s AND email=%s;
    """, (first_name, last_name, email))    
        add_phone(conn, cur.fetchall()[0][0], phone)    

# функция, позволяющая добавить телефон для существующего клиента #

def add_phone(conn, client_id, phone):
    cur.execute("""
    INSERT INTO phone_client(client_id, phone) VALUES(%s, %s);
    """, (client_id, phone))

# функция, позволяющая изменить данные о клиенте #   
    
def change_client(conn, client_id, first_name=None, last_name=None, email=None):
    arg_list = {"first_name": first_name, "last_name": last_name, 'email': email}
    for key, arg in arg_list.items():
        if arg:
            cur.execute(SQL("UPDATE client SET {}=%s WHERE client_id=%s").format(Identifier(key)), (arg, client_id))

# функция, позволяющая удалить телефон для существующего клиента #             

def delete_phone(conn, phone, client_id):
    cur.execute("""
    DELETE FROM phone_client 
    WHERE phone=%s AND client_id=%s;
    """, (phone, client_id))  

# функция, позволяющая удалить существующего клиента #

def delete_client(conn, client_id):
    cur.execute("""
    DELETE FROM phone_client 
    WHERE client_id=%s;
    """, (client_id,))
    cur.execute("""
    DELETE FROM client 
    WHERE client_id=%s;
    """, (client_id,))

# функция, позволяющая найти клиента по его данным: id, имени, фамилии, email или телефону #

def find_client(conn, client_id=None, first_name=None, last_name=None, email=None, phone=None):
    cur.execute("""
    SELECT * FROM client 
    WHERE client_id=%s OR first_name=%s OR last_name=%s OR email=%s;
    """, (client_id, first_name, last_name, email))  
    if phone:
        cur.execute("""
        SELECT client_id FROM phone_client
        WHERE phone=%s;
        """, (phone,)) 
        find_client(conn, client_id=cur.fetchall()[0][0])
        return
    print(*cur.fetchall()) 

 # функция, позволяющая найти номер телефона клиента по его данным: id, имени, фамилии, email #   

def find_phone(conn, client_id=None, first_name=None, last_name=None, email=None):
    cur.execute("""
    SELECT phone from phone_client 
    WHERE client_id=%s;
    """, (client_id,)) 
    if first_name or last_name or email:
        cur.execute("""
        SELECT client_id FROM client
        WHERE first_name=%s OR last_name=%s OR email=%s;
    """, (first_name, last_name, email)) 
        find_phone(conn, client_id=cur.fetchall()[0][0]) 
        return
    print(*cur.fetchall())

conn = psycopg2.connect(database='base_clients', user='postgres', password='12345678')
with conn.cursor() as cur:

    create_table(conn)
    add_client(conn, 'Алекcей', 'Никифоров', 'alex_nikiforov@mail.ru', '89260218963')
    add_client(conn, 'Дмитрий', 'Фитисов', 'dima_fitisov@ya.ru')
    add_client(conn, 'Виктор', 'Иванов', 'vik_ivanov@gmail.com', '89782698430')
    add_phone(conn, '1', '89166593647')
    add_phone(conn, '2', '89166593023')
    add_phone(conn, '3', '89878896358')
    add_phone(conn, '3', '89878895859')
    change_client(conn, '1', email='alex_nikifor89@ya.ru')
    delete_phone(conn, '89878896358', '3')
    delete_client(conn, '3') 
    find_client(conn, phone='89260218963')
    find_client(conn, email='dima_fitisov@ya.ru')
    find_phone(conn, email='alex_nikifor89@ya.ru')
    drop_tables(conn)

    conn.commit()
    conn.close()