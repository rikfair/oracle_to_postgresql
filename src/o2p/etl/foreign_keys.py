#!/usr/bin/python3
# -----------------------------------------------
"""
    DESCRIPTION:
        Foreign key module

    ASSUMPTIONS:
        No assumptions to note

    ACCURACY:
        No accuracy issues to note
"""
# -----------------------------------------------


import o2p.etl

# -----------------------------------------------


def _process_create_fks(table_name, table_names, file=None):
    """
    Builds the primary and unique key constraint statements
    """

    def add_fk_constraint(cn, tcols, rtab, rcols):
        """ Adds a foreign key constraint """

        if cn:
            o2p.execute_command(
                file=file,
                cmd=(
                    f"ALTER TABLE {o2p.PARAMETERS['schema']}.{table_name} ADD CONSTRAINT {cn} FOREIGN KEY "
                    + f"({','.join(tcols)}) REFERENCES {rtab} "
                    + f"({','.join(rcols)});"
                )
            )

    # ---

    query = ' '.join([
        'SELECT uc.constraint_name, ruc.table_name r_table_name, ucc.column_name, rucc.column_name r_column_name',
        '  FROM user_constraints uc, user_cons_columns ucc, user_constraints ruc, user_cons_columns rucc',
        ' WHERE uc.constraint_name = ucc.constraint_name',
        '   AND uc.r_constraint_name = ruc.constraint_name',
        '   AND ruc.constraint_name = rucc.constraint_name',
        '   AND ucc.position = rucc.position',
        f"  AND uc.table_name = '{table_name}'",
        ' ORDER BY uc.constraint_name, ucc.position'
    ])

    dd, cd = o2p.execute_query(query)

    tcolumns = list()
    rcolumns = list()
    constraint_name = ''
    rtable_name = ''

    for con in dd:
        if con[cd['r_table_name']] in table_names:
            if constraint_name != con[cd['constraint_name']]:
                add_fk_constraint(constraint_name, tcolumns, rtable_name, rcolumns)
                constraint_name = con[cd['constraint_name']]
                rtable_name = con[cd['r_table_name']]
                tcolumns = list()
                rcolumns = list()
            tcolumns.append(con[cd['column_name']])
            rcolumns.append(con[cd['r_column_name']])

    add_fk_constraint(constraint_name, tcolumns, rtable_name, rcolumns)


# -----------------------------------------------


def main(table_name, table_names):
    """
    Creates a set of sql files for creating tables in postgresql
    """

    print(f'Running... {table_name} Foreign Keys')

    if o2p.PARAMETERS['datafiles']:
        file_name = o2p.PARAMETERS['datafiles'] + f'/{table_name}.3.sql'
        with open(file_name, 'a', encoding=o2p.PARAMETERS['encoding']) as file:
            file.write(f'\n\\echo Running... {table_name} Foreign Keys\n\n')
            _process_create_fks(table_name, table_names, file)
    else:
        _process_create_fks(table_name, table_names)


# -----------------------------------------------
# End.
