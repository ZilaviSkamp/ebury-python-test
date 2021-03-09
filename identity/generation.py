# -*- coding: utf-8 -*-
import random

import pymysql

from .constants import ID_CHARACTERS
from .db import create_trade_id_table, MYSQL_PARAMETERS

create_trade_id_table()


def generate(count_of_ids=1):
    connection = pymysql.connect(**MYSQL_PARAMETERS)
    cursor = connection.cursor()

    generated_ids = set()
    generate_random_alphanumerics(generated_ids, count_of_ids)

    while True:
        try:
            cursor.execute(f'''
                INSERT INTO trades (trade_id) 
                VALUES {generate_sql_syntax_for_insert_clause(generated_ids)}
            ''')
            connection.commit()
            break
        except pymysql.IntegrityError:
            regenerate_duplicated_ids(generated_ids, count_of_ids, cursor)
            continue

    connection.close()
    return generated_ids.pop() if len(generated_ids) == 1 else generated_ids


def generate_bulk(count_of_ids=1):
    return generate(count_of_ids)


def generate_random_alphanumerics(alphanumerics, count=1):
    while len(alphanumerics) < count:
        alphanumeric = ''.join(random.choices(ID_CHARACTERS, k=7))
        alphanumerics.add(alphanumeric)


def regenerate_duplicated_ids(generated_ids, count_of_ids, cursor):
    if len(generated_ids) == 1:
        generated_ids.clear()
    else:
        cursor.execute(f'''
            SELECT trade_id
            FROM trades
            WHERE trade_id IN ({generate_sql_syntax_for_in_clause(generated_ids)})
        ''')
        duplicated_ids = [ids[0] for ids in cursor.fetchall()]
        generated_ids.difference_update(duplicated_ids)

    generate_random_alphanumerics(generated_ids, count_of_ids)


def generate_sql_syntax_for_in_clause(generated_ids):
    return ', '.join(f'\'{gen_id}\'' for gen_id in generated_ids)


def generate_sql_syntax_for_insert_clause(generated_ids):
    return ', '.join(f'(\'{gen_id}\')' for gen_id in generated_ids)
