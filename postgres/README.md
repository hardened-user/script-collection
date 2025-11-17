# Postgres



## pg_alter_owner.py

Change ownership of the database and all its objects to a new owner.<br/>
There are functional differences between ALTER and REASSIGN OWNER in their implementation.

Requirements:
* Python >= 3.9
* psycopg2

Help
```
./pg_alter_owner.py --help
```

Example
```
PGPASSWORD=***** ./pg_alter_owner.py --host localhost --user postgres target_database new_role
```

---


## pg_backup.py

Postgres databases backup with `pg_dumpall`, `pg_dump` utils.

Requirements:
* Python >= 3.9
* Utils: psql, pg_dump, pg_dumpall

For authentication use `PGPASSWORD` or `~/.pgpass`.

Help
```
./pg_backup.py --help
```

Example
```
PGPASSWORD=***** ./pg_backup.py -h localhost /backup
```

See also [WiKi](https://wiki.enchtex.info/handmade/postgres/pg_backup).

---
