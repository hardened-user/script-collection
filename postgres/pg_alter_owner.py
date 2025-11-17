#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------------------------------------------------
import argparse
import os
import sys
import textwrap
import traceback

import psycopg2
import psycopg2.extras


def main():
    # __________________________________________________________________________
    # command-line options, arguments
    try:
        parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                         description=textwrap.dedent('''\
                                         Change ownership of the database and all it's objects to a new owner.'''))
        parser.add_argument('--host', action='store', type=str,
                            **argparse_required_or_environment('PGHOST', 'localhost'),
                            metavar='', help="database server host (PGHOST) default: localhost")
        parser.add_argument('--port', action='store', type=int,
                            **argparse_required_or_environment('PGPORT', 5432),
                            metavar='', help="database server port (PGPORT) default: 5432")
        parser.add_argument('--username', action='store', type=str,
                            **argparse_required_or_environment('PGUSER', 'postgres'),
                            metavar='', help="database user name (PGUSER) default: postgres")
        parser.add_argument('--password', action='store', type=str,
                            default=os.getenv('PGPASSWORD'),
                            metavar='', help="database user password (PGPASSWORD)")
        parser.add_argument('dbname', action='store', type=str,
                            metavar='<DBNAME>', help="database name")
        parser.add_argument('role', action='store', type=str,
                            metavar='<ROLE>', help="new owner role")
        parser.add_argument('-n', '--dry-run', action='store_true',
                            help="testing mode with no changes made")
        args = parser.parse_args()
    except SystemExit:
        return False
    # ------------------------------------------------------------------------------------------------------------------
    if args.dry_run:
        print("[WW] DRY RUN MODE", flush=True)
    # ==================================================================================================================
    # ==================================================================================================================
    # Start
    # ==================================================================================================================
    try:
        pg_conn = psycopg2.connect(host=args.host,
                                   port=args.port,
                                   user=args.username,
                                   password=args.password,
                                   database=args.dbname,
                                   connect_timeout=10)
    except (psycopg2.OperationalError, psycopg2.ProgrammingError) as err:
        print(f"[EE] Postgres Exception :: {type(err)}\n{str(err).strip()}", flush=True)
        return None
    except Exception as err:
        print(f"[!!] Exception: {type(err)}\n{''.join(traceback.format_exc(limit=1))}", flush=True)
        return None
    print(f"[OK] Postgres successfully connected: {args.dbname}", flush=True)
    # __________________________________________________________________________
    # database
    _sql = '''SELECT pg_catalog.pg_authid.rolname FROM pg_catalog.pg_authid
    JOIN pg_catalog.pg_database ON pg_catalog.pg_authid.oid = pg_catalog.pg_database.datdba
    WHERE pg_catalog.pg_database.datname = current_database();'''
    owner = psql(pg_conn, _sql)
    if owner is None:
        return False
    if not owner:
        owner = [("?????",)]
    print("[--] {}".format('-' * 95), flush=True)
    print(f"[..] Change of database owner ...", flush=True)
    _sql = '''ALTER DATABASE "{}" OWNER TO "{}";'''.format(args.dbname, args.role)
    print(f"\t{_sql.ljust(110)}   # {owner[0][0]} -> {args.role}", flush=True)
    if not args.dry_run:
        _tmp = psql(pg_conn, _sql)
        if _tmp is None:
            return False
    # __________________________________________________________________________
    # schemas
    _sql = '''SELECT DISTINCT "schema_name", "schema_owner" FROM information_schema.schemata 
    WHERE NOT "schema_name" IN ('pg_catalog', 'information_schema', 'pg_toast')
    ORDER BY "schema_name";'''
    schemas = psql(pg_conn, _sql)
    if schemas is None:
        return False
    print("[--] {}".format('-' * 95), flush=True)
    print("[..] Change of schemas owner [{}] ...".format(len(schemas)), flush=True)
    for x in schemas:
        _sql = '''ALTER SCHEMA "{}" OWNER TO "{}";'''.format(x[0], args.role)
        print(f"\t{_sql.ljust(110)}   # {x[1]} -> {args.role}", flush=True)
        if not args.dry_run:
            _tmp = psql(pg_conn, _sql)
            if _tmp is None:
                return False
    # __________________________________________________________________________
    # tables
    _sql = '''SELECT schemaname, tablename, tableowner FROM pg_tables
    WHERE NOT schemaname IN ('pg_catalog', 'information_schema', 'pg_toast')
    ORDER BY schemaname, tablename;'''
    tables = psql(pg_conn, _sql)
    if tables is None:
        return tables
    print("[--] {}".format('-' * 95), flush=True)
    print("[..] Change of tables owner [{}] ...".format(len(tables)), flush=True)
    for x in tables:
        _sql = '''ALTER TABLE "{}"."{}" OWNER TO "{}";'''.format(x[0], x[1], args.role)
        print(f"\t{_sql.ljust(110)}   # {x[2]} -> {args.role}", flush=True)
        if not args.dry_run:
            _tmp = psql(pg_conn, _sql)
            if _tmp is None:
                return False
    # __________________________________________________________________________
    # sequences
    _sql = textwrap.dedent('''
        SELECT 
          n.nspname as sequence_schema,
          c.relname as sequence_name, 
          r.rolname as sequence_owner
        FROM pg_class c
        JOIN pg_roles r ON r.oid = c.relowner
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'S'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        ORDER BY n.nspname, c.relname;
    ''').strip()
    sequences = psql(pg_conn, _sql)
    if sequences is None:
        return False
    print("[--] {}".format('-' * 95), flush=True)
    print("[..] Change of sequences owner [{}] ...".format(len(sequences)), flush=True)
    for x in sequences:
        _sql = '''ALTER SEQUENCE "{}"."{}" OWNER TO "{}";'''.format(x[0], x[1], args.role)
        print(f"\t{_sql.ljust(110)}   # {x[2]} -> {args.role}", flush=True)
        if not args.dry_run:
            _tmp = psql(pg_conn, _sql)
            if _tmp is None:
                return False
    # __________________________________________________________________________
    # views
    _sql = textwrap.dedent('''
        SELECT 
          n.nspname as view_schema,
          c.relname as view_name, 
          r.rolname as view_owner
        FROM pg_class c
        JOIN pg_roles r ON r.oid = c.relowner
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'v'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        ORDER BY n.nspname, c.relname;
    ''').strip()
    views = psql(pg_conn, _sql)
    if views is None:
        return False
    print("[--] {}".format('-' * 95), flush=True)
    print("[..] Change of views owner [{}] ...".format(len(views)), flush=True)
    for x in views:
        _sql = '''ALTER VIEW "{}"."{}" OWNER TO "{}";'''.format(x[0], x[1], args.role)
        print(f"\t{_sql.ljust(110)}   # {x[2]} -> {args.role}", flush=True)
        if not args.dry_run:
            _tmp = psql(pg_conn, _sql)
            if _tmp is None:
                return False
    # __________________________________________________________________________
    # materialized views
    _sql = textwrap.dedent('''
        SELECT 
          n.nspname as materialized_view_schema,
          c.relname as materialized_view_name, 
          r.rolname as materialized_view_owner
        FROM pg_class c
        JOIN pg_roles r ON r.oid = c.relowner
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'm'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        ORDER BY n.nspname, c.relname;
    ''').strip()
    views = psql(pg_conn, _sql)
    if views is None:
        return False
    print("[--] {}".format('-' * 95), flush=True)
    print("[..] Change of materialized views owner [{}] ...".format(len(views)), flush=True)
    for x in views:
        _sql = '''ALTER MATERIALIZED VIEW "{}"."{}" OWNER TO "{}";'''.format(x[0], x[1], args.role)
        print(f"\t{_sql.ljust(110)}   # {x[2]} -> {args.role}", flush=True)
        if not args.dry_run:
            _tmp = psql(pg_conn, _sql)
            if _tmp is None:
                return False
    # ==================================================================================================================
    # ==================================================================================================================
    # End
    # ==================================================================================================================
    # __________________________________________________________________________
    return True


# ======================================================================================================================
# Functions
# ======================================================================================================================
def argparse_required_or_environment(key, default=None):
    return {'default': os.getenv(key, default)} if os.getenv(key, default) else {'required': True}


def pg_query(conn, query):
    cursor = conn.cursor()
    try:
        cursor.execute(query)
    except (psycopg2.DataError, psycopg2.ProgrammingError) as err:
        print(f"[EE] Postgres Exception :: {type(err)}\n{str(err).strip()}", flush=True)
        conn.rollback()
        cursor.close()
        return None
    except Exception as err:
        print(f"[!!] Exception: {type(err)}\n{''.join(traceback.format_exc(limit=1))}", flush=True)
        return None
    else:
        conn.commit()
    # __________________________________________________________________________
    return cursor  # <class 'psycopg2.extensions.cursor'>


def psql(conn, query: str):
    cursor = pg_query(conn, query)
    if cursor is not None:
        if cursor.description is None:
            return []
        else:
            return cursor.fetchall()  # <class 'list'>
    # __________________________________________________________________________
    return None


# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
if __name__ == '__main__':
    # __________________________________________________________________________
    sys.exit(not main())  # Compatible return code
