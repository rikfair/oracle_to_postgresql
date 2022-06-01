#!/usr/bin/python3
# -----------------------------------------------
"""
    DESCRIPTION:
        Triggers module

    ASSUMPTIONS:
        No assumptions to note

    ACCURACY:
        No accuracy issues to note
"""
# -----------------------------------------------

import o2p.etl

# -----------------------------------------------

MOD_COLUMNS = {'created_by', 'created_date', 'modified_by', 'modified_date'}

# -----------------------------------------------


def _get_pgsised_code(text):
    """ Replace Oracle code strings """

    for k, v in o2p.PARAMETERS['pls2pgs'].items():
        text = text.replace(k, v).replace(k.lower(), v)

    return text


# -----------------------------------------------


def _process_create_triggers(table_name, table_columns, file=None):
    """
    Builds the triggers
    """

    tcset = set(table_columns)

    query = ' '.join([
        "SELECT ut.trigger_name, ut.trigger_type, ut.triggering_event, ut.when_clause",
        "  FROM user_triggers ut",
        " WHERE ut.trigger_name NOT LIKE '%SEQTRG'",
        "   AND ut.trigger_name NOT LIKE '%AU_TRG'"
        f"  AND ut.table_name = '{table_name}'"
    ])

    dd, _ = o2p.execute_query(query)
    triggers = {t[0]: t for t in dd}

    for tn in triggers:
        if tn not in o2p.PARAMETERS['ignore_triggers']:
            if tn.endswith('_MODTRG') and MOD_COLUMNS.issubset(tcset):
                _process_mod_trigger(table_name, tn, file)
            else:
                function_name = triggers[tn][0].replace('trg', 'fnc')  # TODO Only the last replace.
                _process_trg_function(triggers[tn], function_name, file)
                _process_trg_trigger(table_name, triggers[tn], function_name, file)


# -----------------------------------------------


def _process_mod_trigger(table_name, trigger_name, file):
    """ Builds a default mod trigger """

    cmd = '\n'.join([
        f'CREATE TRIGGER {trigger_name}',
        f'BEFORE INSERT OR UPDATE ON {table_name}',
        'FOR EACH ROW',
        'EXECUTE PROCEDURE mod_trigger_fnc(); \n\n'
    ])

    o2p.execute_command(file=file, cmd=cmd)


# -----------------------------------------------


def _process_trg_function(trigger, function_name, file):
    """ Builds a trigger function from the Oracle code """

    query = ' '.join([
        "SELECT us.text",
        "  FROM user_source us",
        f"WHERE us.name = '{trigger[0]}'",
        " ORDER BY us.line"
    ])

    dd = [d[0] for d in o2p.execute_query(query)[0]]
    body = False
    text = ''

    for usl in dd:
        if not body and usl.strip().upper() in ['DECLARE', 'BEGIN']:
            body = True
        if body:
            if usl.strip().upper() == 'END;':
                text += '  RETURN NEW; \n'
            text += _get_pgsised_code(usl)
        elif usl.strip().upper().endswith(' DECLARE'):
            text += 'DECLARE \n'
            body = True
        elif usl.strip().upper().endswith(' BEGIN'):
            text += 'BEGIN \n'
            body = True

    cmd = '\n'.join([
        f"CREATE FUNCTION {function_name}()",
        "RETURNS TRIGGER",
        "LANGUAGE PLPGSQL",
        "AS",
        "$$",
        text,
        "$$; \n\n"
    ])

    o2p.execute_command(file=file, cmd=cmd)


# -----------------------------------------------


def _process_trg_trigger(table_name, trigger, function_name, file):
    """ Creates the trigger stub """

    ba = trigger[1].split(' ', 1)[0]
    er = 'FOR EACH ROW' if trigger[1].endswith('EACH ROW') else ''
    wc = 'WHEN (' + trigger[3] + ')' if trigger[3] else ''

    cmd = '\n'.join([
        f'CREATE TRIGGER {trigger[0]}',
        f'{ba} {trigger[2]} ON {table_name} {er} {wc}',
        f'EXECUTE PROCEDURE {function_name}(); \n\n'
    ])

    o2p.execute_command(file=file, cmd=cmd)


# -----------------------------------------------


def main(table_name, table_columns):
    """
    Creates a set of sql files for creating tables in postgresql
    """

    print(f'Running... {table_name} Triggers')

    if o2p.PARAMETERS['datafiles']:
        file_name = o2p.PARAMETERS['datafiles'] + f'/{table_name}.3.sql'
        with open(file_name, 'a', encoding=o2p.PARAMETERS['encoding']) as file:
            file.write(f'\n\\echo Running... {table_name} Triggers\n\n')
            _process_create_triggers(table_name, table_columns, file)
    else:
        _process_create_triggers(table_name, table_columns, None)


# -----------------------------------------------
# End.
