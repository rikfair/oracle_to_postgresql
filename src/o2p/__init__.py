#!/usr/bin/python3
# -----------------------------------------------
"""
    DESCRIPTION:
        Package level functions

    ASSUMPTIONS:
        Oracle instantclient 12.1 is installed

    ACCURACY:
        No accuracy issues to note
"""
# -----------------------------------------------

import json
import os

import cx_Oracle
import psycopg2

# -----------------------------------------------


def _get_parameters():
    """ Gets the parameters """

    pf1 = f'{HOME}/parameters.json'
    pf2 = f'{HOME}/_parameters.json'

    with open(pf1 if os.path.isfile(pf1) else pf2, encoding='utf-8-sig') as jf:
        data = json.load(jf)

    data['schema'] = data['oracle'].split('/', 1)[0]
    data['pls2pgs'][f" {data['schema'].upper()}."] = ' '

    return data


# -----------------------------------------------


def _initialise_postgres():
    """ Initialises the Postgres connection """

    if not PARAMETERS['direct']:
        return None

    conn = psycopg2.connect(PARAMETERS['postgres'])
    conn.set_session(autocommit=True)
    conn.cursor().execute(f"DROP SCHEMA IF EXISTS {PARAMETERS['schema']} CASCADE")
    conn.cursor().execute(f"CREATE SCHEMA {PARAMETERS['schema']}")
    conn.cursor().execute(f"SET search_path TO {PARAMETERS['schema']}")

    return conn


# -----------------------------------------------


def output_type_handler(cursor, name, defaultType, size, precision, scale):  # noqa
    """ Converts Clob to Long to improve performance. Parameter names and order are specified by cx_Oracle """

    if defaultType == cx_Oracle.CLOB:
        return cursor.var(cx_Oracle.LONG_STRING, arraysize=cursor.arraysize)


# -----------------------------------------------

HOME = os.path.dirname(os.path.dirname(__file__))
PARAMETERS = _get_parameters()

cx_Oracle.init_oracle_client(lib_dir=PARAMETERS['oracle_instant_client'])
OCONN = cx_Oracle.connect(PARAMETERS['oracle'], encoding='UTF-8')
OCONN.outputtypehandler = output_type_handler

PCONN = _initialise_postgres()

# -----------------------------------------------


def execute_command(file, cmd):
    """
    Execute command and write to a file
    """

    if file:
        file.write(cmd + '\n')
    if PARAMETERS['direct']:
        PCONN.cursor().execute(cmd)


# -----------------------------------------------


def execute_query(query):
    """
    Returns tuple of dd, dictionary data, and cd, cursor description
    """

    cursor = OCONN.cursor()
    cursor.execute(query)
    cd = {c[0].lower(): i for i, c in enumerate(list(cursor.description))}
    dd = list(cursor.fetchall())
    cursor.close()

    return dd, cd


# -----------------------------------------------


def read_file_to_string(f):
    """ Reads a file and return the file text as a string """

    text = ''
    with open(f, 'r', encoding='utf-8-sig') as r:
        text += r.read()
    return text


# -----------------------------------------------


def write_file_from_string(f, s, m='a', encoding='utf-8'):
    """
    Function to read a file and return the file text as a string
    :param f: File name
    :param s: String to write
    :param m: Mode, 'a'ppend, or 'w'rite
    :param encoding: The encoding of the file
    """

    f = f.replace('%HOME%', HOME)
    with open(f, m, encoding=encoding) as text_file:
        text_file.write(s)  # Use write instead of print to avoid trailing new line


# -----------------------------------------------
# End.
