# oracle_to_postgresql
Copy tables, data, indexes, sequences and (some) triggers from Oracle to PostgreSQL

python 3.7

# Background

This project was created to move a specific Oracle database to PostgreSQL. The application was moving from an Oracle Database, with PLSQL and Apex platform, to PostgreSQL with application code re-written in Python. Hence this project covers only the areas involved with data, such as the tables, indexes, sequences and simple triggers for data integrity.

# Audience

This is for you if:
- You want to copy a schema in Oracle to PostgreSQL
- You don't need automated conversion of any PL/SQL functions, procedures or packages
- Everything is contained within a single schema
- Your application can have a single tablespace for tables and another for indexes.

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
- oracle_instant_client: The path to the Oracle Instant Client executable. Tested with 12.1
- postgres: The PostgreSQL connection string "dbname='<dbname>' user='<username>' host='<host>' password='<password>' port='<port>'"
- ignore_data: A list of tables to not include in the *data* extract.
- ignore_indexes: A list of indexes that we not be required in PostgreSQL
- ignore_triggers: A list of trigger names not to include in the copy
- pls2pgs: This is a dictionary of PL/SQL terms that have a blunt string replace made on them within the trigger code
  
Any tables that don't are not required to be copied are expected to have `DEPRECATED` at the beginning of the table comments.

Similarly, any columns that should not be included in the copy, should have their comment begin with `DO NOT USE`.

# Execute

Run the copy process by executing the `o2p.main.main` function. Pass in any of the parameters to override the parameters file.

The main function follows the steps:
- Create the objects in the prebuild directory.
- Loop through the Oracle tables, creating each in PostgreSQL and inserting data.
- Loop through the Oracle tables, creating the foreign keys, indexes and triggers.
- Create the objects in the postbuild directory. 

# Prebuild and Postbuild Scripts

The prebuild and postbuild directory contain sql scripts.
Each script containing a statement that is executed on PostgreSQL.
Additional scripts can be placed in either of these directories to be executed at runtime.

Some of the prebuild and postbuild scripts are to ease development, so that common Oracle commands will still compile and run in PostgreSQL.
Such as NVL, LAST_DAY, RAISE_APPLICATION_ERROR, TO_CHAR, TO_NUMBER and TRUNC.

The postbuild directory contains an anonymous block to grant privileges on the recently created objects.

# Sequences

The package assumes that if a table's primary key column has a sequence,
then a trigger will exist to populate the sequence.
The dependency views are queried to link a sequence to a table.
No new trigger is created, instead the sequence is linked directly to the primary key column.

# Audit Columns

If tables contain the columns:
- created_by
- created_date
- modified_by
- modified_date

Then a trigger is automatically created to maintain these columns.



