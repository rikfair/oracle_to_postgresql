# oracle_to_postgresql
Copy tables, data, indexes, sequences and (some) triggers from Oracle to PostgreSQL

python 3.7

# Background

This project was created to move a specific Oracle database to PostgreSQL. The application was moving from an Oracle Database, with PLSQL and Apex platform, to PostgreSQL with application code re-written in Python. Hence this project covers only the areas involved with data, such as the tables, indexes, sequences and simple triggers for data integrity.

# Operation

The package will connect to both the Oracle and PostgreSQL servers. From a specified schema, it will create the objects required and load the data. There's also an option to create text files so that they can be backed-up or rerun later.

To ease continuity between Oracle and PostgreSQL development, and to allow our Python code to run in both Oracle and PostgreSQL, some additional objects are created to mimic Oracle functionality in PostgreSQL.

# Parameters

The `_parameters.json` file needs to be copied to `parameters.json` and the values updated for the specific environment.

- datafile_path: The directory to output the data files
- direct: true or false, should the process directly create objects in the PostgreSQL database
- encoding: utf-8, not sure why this is an option, but just in case
- export_data: true or false, should the data be exported too
- insert_rows: how many rows should be inserted into PostgreSQL in each statement. For me 2500 is good.
- oracle: The Oracle connection string "<username>/<password>@<host>:<port>/<dbname>"
- output_to_file: true or false, should files be created
- postgres: The PostgreSQL connection string "dbname='<dbname>' user='<username>' host='<host>' password='<password>' port='<port>'"
- schema: The schema name in Oracle to copy to PostgreSQL
- ignore_data: A list of tables to not include in the *data* extract.
- ignore_indexes: A list of indexes that we not be required in PostgreSQL
- ignore_triggers: A list of trigger names not to include in the copy
- pls2pgs: This is a dictionary of PLSQL terms that have a blunt string replace made on them within the trigger code
  
Any tables that don't are not required to be copied are expected to have `DEPRECATED` at the beginning of the table comments.
  
