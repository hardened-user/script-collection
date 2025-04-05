# Postgres

## pg_backup.py

Backup Postgres databases with `pg_dumpall`, `pg_dump` utils.

Requirements:
* Python >= 3.9
* Utils: psql, pg_dump, pg_dumpall

For authentication use `PGPASSWORD` env or `~/.pgpass` file.

Help
```
./pg_backup.py --help
```

Example
```
PGPASSWORD=***** ./pg_backup.py -h localhost /backup
```

See also [wiki](https://wiki.enchtex.info/handmade/zabbix/zabbix_data_mining) page.

___
