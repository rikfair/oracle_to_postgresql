#!/usr/bin/python3
# -----------------------------------------------
"""
    DESCRIPTION:
        Sequences module

    ASSUMPTIONS:
        No assumptions to note

    ACCURACY:
        No accuracy issues to note
"""
# -----------------------------------------------

import re

import o2p.etl

# -----------------------------------------------


def _get_sequence_column(trigger_name):
    """
    Gets the column to attach the sequence to
    """

    query = ' '.join([
        "SELECT us.text",
        "  FROM user_source us",
        f"WHERE us.name = '{trigger_name}'",
    ])

    cursor = o2p.OCONN.cursor()
    cursor.execute(query)
    data = list(cursor.fetchall())
    cursor.close()

    for d in data:
        columns = re.findall(r'NEW\.(.*):=', d[0].replace(' ', '').upper())
        if len(columns) == 1:
            return columns[0].lower()

    return None


# -----------------------------------------------


def _get_sequences(table_names):
    """
    Gets a list of table names to export
    """

    query = ' '.join([
        "SELECT ut.table_name, us.sequence_name, us.last_number, ut.trigger_name",
        "  FROM user_triggers ut,",
        "       user_dependencies ud,",
        "       user_sequences us",
        " WHERE ut.trigger_name LIKE '%SEQTRG'",
        "   AND ut.trigger_name = ud.name",
        "   AND ud.referenced_type = 'SEQUENCE'",
        "   AND ud.referenced_name = us.sequence_name"
    ])

    cursor = o2p.OCONN.cursor()
    cursor.execute(query)
    data = list(cursor.fetchall())
    cursor.close()

    for i, d in enumerate(data):
        data[i] = [*d, _get_sequence_column(d[3])]

    table_sequences = {
        d[0]: {'sequence_name': d[1], 'last_number': d[2], 'column_name': d[4]}
        for d in data if d[0] in table_names and d[4]
    }

    return table_sequences


# -----------------------------------------------


def _process_sequences(sequences, file=None):
    """ Process the sequence list """

    schema = o2p.PARAMETERS['schema']

    for tn in sorted(list(sequences)):
        seq = sequences[tn]
        o2p.execute_command(
            file=file,
            cmd=f"CREATE SEQUENCE {schema}.{seq['sequence_name']} INCREMENT 1 START {seq['last_number'] + 1};"
        )


# -----------------------------------------------


def get_sequences(table_names):
    """
    Creates a set of sql files for creating sequences in postgresql
    """

    print('Creating Sequences')

    sequences = _get_sequences(table_names)

    if o2p.PARAMETERS['datafiles']:
        file_name = o2p.PARAMETERS['datafiles'] + '/sequences.sql'
        with open(file_name, 'w', encoding=o2p.PARAMETERS['encoding']) as file:
            file.write('\\echo Creating Sequences\n\n')
            _process_sequences(sequences, file)
    else:
        _process_sequences(sequences)

    return sequences


# -----------------------------------------------
# End.
