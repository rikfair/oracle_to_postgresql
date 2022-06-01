#!/usr/bin/python3
# -----------------------------------------------
"""
    DESCRIPTION:
        Tables module

    ASSUMPTIONS:
        No assumptions to note

    ACCURACY:
        No accuracy issues to note
"""
# -----------------------------------------------

import o2p.etl

# -----------------------------------------------


def _process_create_comments_cols(table_name, file):
    """
    Builds the create column comment statements
    """

    schema = o2p.PARAMETERS['schema']

    query = ' '.join([
        "SELECT column_name, REPLACE(comments, '''', '''''') comments",
        "  FROM user_col_comments",
        f"WHERE table_name = '{table_name}' AND comments NOT LIKE 'DO NOT USE%'"
    ])

    dd, cd = o2p.execute_query(query)

    # ---

    for d in dd:
        comments = d[cd['comments']]
        if comments:
            comments = comments.replace('\n', ' ')
            o2p.execute_command(
                file=file,
                cmd=f"COMMENT ON COLUMN {schema}.{table_name}.{d[cd['column_name']]} IS '{comments}';"
            )


# -----------------------------------------------


def _process_create_comments_tab(table_name, file):
    """
    Builds the create table comment statement
    """

    query = ' '.join([
        "SELECT REPLACE(comments, '''', '''''') comments",
        "  FROM user_tab_comments",
        f"WHERE table_name = '{table_name}'"
    ])

    dd, cd = o2p.execute_query(query)

    # ---

    for d in dd:
        comments = d[cd['comments']]
        if comments:
            comments = comments.replace('\n', ' ')
            o2p.execute_command(
                file=file,
                cmd=f"\nCOMMENT ON TABLE {o2p.PARAMETERS['schema']}.{table_name} IS '{comments}';\n"
            )


# -----------------------------------------------


def _process_create_puks(table_name, file):
    """
    Builds the primary and unique key constraint statements
    """

    def add_puk_constraint(cn, ct, cols):
        """ Executes the statement to create the primary key """
        if cn:
            o2p.execute_command(
                file=file,
                cmd=(
                    f"ALTER TABLE {o2p.PARAMETERS['schema']}.{table_name} ADD CONSTRAINT {cn} "
                    + f"{'PRIMARY KEY' if ct == 'P' else 'UNIQUE'} ("
                    + ','.join(cols) + f") USING INDEX TABLESPACE {o2p.PARAMETERS['tablespace_indexes']};"
                )
            )

    # ---

    query = ' '.join([
        "SELECT uc.constraint_name, uc.constraint_type, ucc.column_name",
        "  FROM user_constraints uc, user_cons_columns ucc",
        " WHERE uc.constraint_type IN ('P','U')"
        f"  AND uc.table_name = '{table_name}'"
        "   AND uc.constraint_name = ucc.constraint_name"
        " ORDER BY uc.constraint_name, ucc.position"
    ])

    dd, cd = o2p.execute_query(query)

    columns = list()
    constraint_name = ''
    constraint_type = ''

    for con in dd:
        if constraint_name != con[cd['constraint_name']]:
            add_puk_constraint(constraint_name, constraint_type, columns)
            constraint_name = con[cd['constraint_name']]
            constraint_type = con[cd['constraint_type']]
            columns = list()
        columns.append(con[cd['column_name']])

    add_puk_constraint(constraint_name, constraint_type, columns)


# -----------------------------------------------


def _process_create_table(table_name, sequence, table_columns, file):
    """
    Builds the create table statement
    """

    lines = list()

    # ---

    query = ' '.join([
        f"SELECT LOWER(column_name) column_name, data_type, data_length, data_precision, data_scale, nullable",
        "   FROM user_tab_columns",
        f" WHERE table_name = '{table_name}'",
        "    AND column_name NOT IN",
        "        (SELECT column_name FROM user_col_comments"
        f"         WHERE table_name = '{table_name}' AND comments LIKE 'DO NOT USE%')",
        "  ORDER BY column_id"
    ])

    dd, cd = o2p.execute_query(query)

    # ---

    for column in dd:

        column_name = column[cd['column_name']]
        if column[cd['column_name']] in ['limit', 'natural']:
            column_name = '"' + column_name + '"'

        table_columns.append(column_name)
        line = f", {column_name}".ljust(35)

        if column[cd['data_type']] == 'NUMBER' and column_name.endswith('_id'):
            line += "INTEGER"
        elif column[cd['data_type']] in ['CHAR', 'VARCHAR2']:
            line += f"VARCHAR({column[cd['data_length']]})"
        elif column[cd['data_type']] == 'NUMBER' and column[cd['data_precision']] and column[cd['data_scale']]:
            line += f"NUMERIC({column[cd['data_precision']]},{column[cd['data_scale']]})"
        elif column[cd['data_type']] == 'NUMBER' and column[cd['data_precision']]:
            if column[cd['data_precision']] < 5:
                line += "SMALLINT"
            elif column[cd['data_precision']] > 9:
                line += "BIGINT"
            else:
                line += "INTEGER"
            if not column_name.endswith('_id') and column[cd['data_precision']] < 9:
                line += f" CHECK ({column_name} < {'1' + ('0' * column[cd['data_precision']])})"
        elif column[cd['data_type']] == 'NUMBER':
            line += f"NUMERIC"
        elif column[cd['data_type']] == 'DATE':
            line += "TIMESTAMP(0)"
        elif column[cd['data_type']] == 'CLOB':
            line += "TEXT"
        elif column[cd['data_type']] == 'BLOB':
            line += "BYTEA"
        else:
            print(f"Unknown column: {table_name}.{column_name}")
            line += f"UNKNOWN"

        if column[cd['nullable']] == 'N':
            line += ' NOT NULL'

        if sequence and column_name == sequence['column_name']:
            line += f" DEFAULT NEXTVAL('{sequence['sequence_name']}')"

        lines.append(line)

    # ---

    o2p.execute_command(
        file=file,
        cmd=(
            f"CREATE TABLE {o2p.PARAMETERS['schema']}.{table_name.lower()} \n("
            + ('\n'.join(lines)).lstrip(',')
            + f"\n) TABLESPACE {o2p.PARAMETERS['tablespace_tables']}\n;\n"
        )
    )


# -----------------------------------------------


def _process_tables(table_name, sequence, table_columns, file=None):
    """ Process the table """

    _process_create_table(table_name, sequence, table_columns, file)
    _process_create_puks(table_name, file)
    _process_create_comments_tab(table_name, file)
    _process_create_comments_cols(table_name, file)


# -----------------------------------------------


def main(table_name, sequence, table_columns):
    """
    Creates a set of sql files for creating tables in postgresql
    """

    print(f'Running... {table_name} Tables')

    if o2p.PARAMETERS['datafiles']:
        file_name = o2p.PARAMETERS['datafiles'] + f'/{table_name}.1.sql'
        with open(file_name, 'w', encoding=o2p.PARAMETERS['encoding']) as file:
            file.write(f'\\echo Running... {table_name} Tables \n\n')
            _process_tables(table_name, sequence, table_columns, file)
    else:
        _process_tables(table_name, sequence, table_columns)


# -----------------------------------------------
# End.
