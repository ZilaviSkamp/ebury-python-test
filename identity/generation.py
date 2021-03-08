# -*- coding: utf-8 -*-
import random

import pymysql

from .constants import ID_CHARACTERS
from .db import create_trade_id_table, MYSQL_PARAMETERS

create_trade_id_table()


def generate():
    connection = pymysql.connect(**MYSQL_PARAMETERS)
    cursor = connection.cursor()

    while True:
        generated_id = ''.join(random.choices(ID_CHARACTERS, k=7))

        try:
            cursor.execute(f'INSERT INTO trades (trade_id) VALUES (\'{generated_id}\')')
            connection.commit()
            break
        except pymysql.IntegrityError:
            continue

    connection.close()
    return generated_id
