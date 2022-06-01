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
import os

import o2p.etl.data
import o2p.etl.foreign_keys
import o2p.etl.indexes
import o2p.etl.sequences
import o2p.etl.tables
import o2p.etl.triggers

# -----------------------------------------------


def _get_table_names():
    """
    Gets a list of table names to export
    """

    query = ' '.join([
        "SELECT table_name FROM user_tables",
        " WHERE table_name NOT LIKE '%EXT'",
        "   AND table_name NOT LIKE '%GTT'",
        "   AND table_name NOT LIKE '%MVW'",
        "   AND table_name NOT LIKE 'SYS%'",
        " MINUS ",
        "SELECT table_name FROM user_tab_comments WHERE comments LIKE 'DEPRECATED%'"
    ])

    cursor = o2p.OCONN.cursor()
    cursor.execute(query)
    table_names = [t[0] for t in cursor.fetchall()]
    cursor.close()

    return table_names


# -----------------------------------------------


def _create_script():
    """
    Creates the main script to run
    """

    file_name = o2p.PARAMETERS['datafiles'] + '/_run_.sql'

    script = [
        f"-- Run: \\i '{file_name}' \n",
        f"SET search_path TO {o2p.PARAMETERS['schema']}; \n",
        f"SET client_encoding = 'UTF8'; \n",
        '\\set AUTOCOMMIT on \n',
        '\\ir sequences.sql \n',
        '\\ir scripts.sql \n'
    ]

    files = sorted(os.listdir(o2p.PARAMETERS['datafiles']))

    for ft in ['.1.sql', '.2.sql', '.3.sql']:
        script.append(f'\n\\echo Moving on to "{ft}" files...\n')
        for file in files:
            if file.endswith(ft):
                script.append(f'\\ir {file}')

    script.append('\n')

    o2p.write_file_from_string(file_name, '\n'.join(script))
    print(f"Run: \\i '{file_name}'")


# -----------------------------------------------


def _process_script_directory(prepost, file=None):
    """ Loops through script directory """

    script_dir = os.path.dirname(__file__) + f'/{prepost}build/'
    for fn in os.listdir(script_dir):
        script_txt = o2p.read_file_to_string(script_dir + fn).replace('%%schema%%', o2p.PARAMETERS['schema'])
        o2p.execute_command(file, script_txt)


# -----------------------------------------------


def _process_scripts(prepost):
    """ Creates functions and other objects """

    print(f'Creating {prepost}-build scripts')

    if o2p.PARAMETERS['datafiles']:
        file_name = o2p.PARAMETERS['datafiles'] + f'/{prepost}build.sql'
        with open(file_name, 'w', encoding=o2p.PARAMETERS['encoding']) as file:
            file.write(f'\\echo Creating {prepost}-build scripts\n\n')
            _process_script_directory(prepost, file)
    else:
        _process_script_directory(prepost)


# -----------------------------------------------


def main(**kwargs):
    """
    Creates a set of sql files for creating objects in postgresql
    """

    now = datetime.datetime.now().strftime('%Y-%m-%d#%H-%M-%S')
    print(f'Starting: {now}')

    for k, v in kwargs.items():
        o2p.PARAMETERS[k] = v

    if o2p.PARAMETERS['datafile_path']:
        o2p.PARAMETERS['datafiles'] = o2p.PARAMETERS['datafile_path'] + '/' + now
        os.makedirs(o2p.PARAMETERS['datafiles'])
    else:
        o2p.PARAMETERS['datafiles'] = False

    if 'oracle' in kwargs:
        o2p.PARAMETERS['schema'] = o2p.PARAMETERS['oracle'].split('/', 1)[0]

    # ---

    table_columns = dict()
    table_names = _get_table_names()
    sequences = o2p.etl.sequences.get_sequences(table_names)
    _process_scripts('pre')

    for table_name in table_names:
        print(f'Starting {table_name} (1/2): {datetime.datetime.now()}')
        table_columns[table_name] = []
        o2p.etl.tables.main(table_name, sequences.get(table_name), table_columns[table_name])
        if o2p.PARAMETERS['export_data']:
            o2p.etl.data.main(table_name, table_columns[table_name])

    for table_name in table_names:
        print(f'Starting {table_name} (2/2): {datetime.datetime.now()}')
        o2p.etl.foreign_keys.main(table_name, table_names)
        o2p.etl.indexes.main(table_name)
        o2p.etl.triggers.main(table_name, table_columns[table_name])

    _process_scripts('post')

    # ---

    if o2p.PARAMETERS['datafiles']:
        _create_script()

    # ---

    print(f"Completed: {datetime.datetime.now().strftime('%Y-%m-%d#%H-%M-%S')}")


# -----------------------------------------------

if __name__ == '__main__':
    main(export_data=False, datafile_path=False)

# -----------------------------------------------
# End.
