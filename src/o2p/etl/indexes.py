#!/usr/bin/python3
# -----------------------------------------------
"""
    DESCRIPTION:
        Indexes module

    ASSUMPTIONS:
        No assumptions to note

    ACCURACY:
        No accuracy issues to note
"""
# -----------------------------------------------

import o2p.etl

# -----------------------------------------------


def _process_create_indexes(table_name, file=None):
    """
    Builds the primary and unique key constraint statements
    """

    schema = o2p.PARAMETERS['schema']
    idx_tbs = o2p.PARAMETERS['tablespace_indexes']

    # ---

    def add_index(idx, cols):
        """ Executes the create index statement """
        if idx:
            o2p.execute_command(
                file=file,
                cmd=f"CREATE INDEX {idx} ON {schema}.{table_name} ({','.join(cols)}) TABLESPACE {idx_tbs};"
            )
        return

    # ---

    query = ' '.join([
        "SELECT ui.index_name, uic.column_name",
        "  FROM user_indexes ui, user_ind_columns uic",
        " WHERE ui.uniqueness = 'NONUNIQUE'"
        "   AND ui.index_type = 'NORMAL'"
        f"  AND ui.table_name = '{table_name}'"
        "   AND ui.index_name = uic.index_name"       
        " ORDER BY ui.index_name, uic.column_position"
    ])

    dd, cd = o2p.execute_query(query)

    columns = list()
    index_name = ''

    for d in dd:
        if index_name not in o2p.PARAMETERS['ignore_indexes']:
            if index_name != d[cd['index_name']]:
                add_index(index_name, columns)
                index_name = d[cd['index_name']]
                columns = []
            columns.append(d[cd['column_name']])

    add_index(index_name, columns)


# -----------------------------------------------


def main(table_name):
    """
    Creates a set of sql files for creating tables in postgresql
    """

    print(f'Running... {table_name} Indexes')

    if o2p.PARAMETERS['datafiles']:
        file_name = o2p.PARAMETERS['datafiles'] + f'/{table_name}.3.sql'
        with open(file_name, 'a', encoding=o2p.PARAMETERS['encoding']) as file:
            file.write(f'\n\\echo Running... {table_name} Indexes \n\n')
            _process_create_indexes(table_name, file)
    else:
        _process_create_indexes(table_name)


# -----------------------------------------------
# End.
