import os

import pymysql

MYSQL_PARAMETERS = {
    'host': os.getenv('MYSQL_HOST'),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DATABASE')
}


def create_trade_id_table():
    connection = pymysql.connect(**MYSQL_PARAMETERS)

    with connection.cursor() as cursor:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                trade_id VARCHAR(7) NOT NULL UNIQUE
            )
        ''')

    connection.commit()
    connection.close()

