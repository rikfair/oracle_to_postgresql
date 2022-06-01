#!/usr/bin/python3
# -----------------------------------------------
"""
    DESCRIPTION:
        Insert statement module

    ASSUMPTIONS:
        No assumptions to note

    ACCURACY:
        No accuracy issues to note
"""
# -----------------------------------------------

import datetime
import cx_Oracle
import o2p.etl

# -----------------------------------------------


def _insert_rows(table_name, columns, file, rows):
    """ Build the insert statement when the number of rows has been reached """

    rs = ',\n'.join(rows)
    cmd = f"INSERT INTO {o2p.PARAMETERS['schema']}.{table_name} ({columns}) VALUES \n{rs}; \n"
    cmd = cmd.replace('\x00', '')
    o2p.execute_command(file=file, cmd=cmd)


# -----------------------------------------------


def _process_data(table_name, table_columns, file):
    """ Processes export for a specified table """

    insert_rows = o2p.PARAMETERS['insert_rows']

    columns = ','.join(table_columns)
    query = 'SELECT ' + columns.replace('"', '') + ' FROM ' + table_name
    cursor = o2p.OCONN.cursor()
    cursor.execute(query)

    while True:
        rows = cursor.fetchmany(insert_rows)
        if not rows:
            break
        rows_to_insert = []

        for row in rows:
            cols = []
            for col in row:
                if col is None:
                    cols.append('NULL')
                elif isinstance(col, (str, cx_Oracle.LOB)):
                    cols.append("'" + str(col).replace("'", "''") + "'")
                elif isinstance(col, datetime.datetime):
                    cols.append(f"TO_TIMESTAMP('{col.strftime('%Y%m%d%H%M%S')}','YYYYMMDDHH24MISS')")
                else:
                    cols.append(str(col))
            rows_to_insert.append('(' + ','.join(cols) + ')')

        _insert_rows(table_name, columns, file, rows_to_insert)


# -----------------------------------------------


def _process_main(table_name, table_columns, file=None):

    if table_name not in o2p.PARAMETERS['ignore_data']:
        print(f'Running... {table_name} Data')
        if file:
            file.write(f'\\echo Running... {table_name} Data\n\n')
        _process_data(table_name, table_columns, file)


# -----------------------------------------------


def main(table_name, table_columns):
    """ Main data export function """

    if o2p.PARAMETERS['datafiles']:
        file_name = o2p.PARAMETERS['datafiles'] + f'/{table_name}.2.sql'
        with open(file_name, 'w', encoding=o2p.PARAMETERS['encoding']) as file:
            _process_main(table_name, table_columns, file)
    else:
        _process_main(table_name, table_columns)


# -----------------------------------------------
# End.
