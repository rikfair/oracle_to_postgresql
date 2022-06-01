#  Windows PostgreSQL SetupCommand prompt

At the command prompt log onto postgres

```
"C:\Program Files\PostgreSQL\14\bin\psql" -U postgres
<password>
```

First create the database

```
create database db_<schema>;
```

Let's have a look to see if the new database is there...

```
\l
```

Create an application user and administrator

```
create user <schema>_user with password '<user-password>';
create user <schema>_admin with password '<admin-password>';
```


Next, create the tablespaces. Remember PostgreSQL only allows one tablespace per directory.
And note that the tablespace names here must match those in the `parameters.json` file.

```
create tablespace ts_tables owner <schema>_admin location '<path>\ts_tables';
create tablespace ts_indexes owner <schema>_admin location '<path>\ts_indexes';
```

After that, grant access on the database to the new admin user

```
grant create on database db_<schema> to <schema>_admin;
```

Now change to the new database and admin user

```
\c db_<schema> <schema>_admin
```

Next, create the schema. If this is not the first run through of the script you may need to drop it first.

```
drop schema <schema> cascade;
create schema <schema>;
```

Let's check the schema is there...

```
\dn
```

Finally, we have to grant some privileges to the admin user

```
grant usage on schema <schema> to <schema>_admin;
grant all privileges on schema <schema> to <schema>_admin;
```

Done.
