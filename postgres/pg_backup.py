#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------------------------------------------------
import argparse
import datetime
import hashlib
import os
import re
import socket
import subprocess
import sys
import tempfile
import traceback
from typing import Union

_GLOBALS_NAME = "globals"
_EXCLUDE_BASE = ["postgres", "template0", "template1"]

__START_DT = datetime.datetime.now()
__HOSTNAME = socket.getfqdn()


def main():
    main_return_value = True
    # __________________________________________________________________________
    # command-line options, arguments
    try:
        parser = argparse.ArgumentParser(
            description='Backup Postgres databases with pg_dumpall, pg_dump utils.',
            add_help=False)
        parser.add_argument('path', action='store', type=str,
                            help="backup directory path")
        parser.add_argument('-h', action='store', type=str, default="", dest="host",
                            help="database server host or socket directory (default: local socket)")
        parser.add_argument('-p', action='store', type=int, default=5432, dest="port",
                            help="database server port number (default: 5432)")
        parser.add_argument('-U', action='store', type=str, default="postgres", dest="user",
                            help="connect as specified database user (default: postgres)")
        parser.add_argument('-e', action='append', type=str, default=list(), dest="exclude",
                            help="exclude database")
        parser.add_argument('--compress', action='store', type=str, default="gzip:9", dest="compress",
                            help="specify the compression method and/or the compression level (default: gzip:9)")
        parser.add_argument('-j', action='store', type=int, default=0, dest="njobs",
                            help="use this many parallel jobs to dump (default: 0)")
        parser.add_argument('-n', '--dry-run', action='store_true',
                            help="testing mode with no changes made")
        parser.add_argument('--help', action='help', help='show this help message and exit')
        args = parser.parse_args()  # <class 'argparse.Namespace'>
    except SystemExit:
        return False
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    # Validate
    # ------------------------------------------------------------------------------------------------------------------
    if not (args.path and args.user):
        parser.print_help()
        return False
    #
    if args.path == ".":
        args.path = os.path.abspath(os.path.dirname(__file__))
    else:
        args.path = os.path.abspath(args.path)
    #
    args.exclude = map(lambda x: x.strip(), args.exclude)
    args.exclude = list(filter(lambda x: x, args.exclude))
    args.exclude = set(args.exclude + _EXCLUDE_BASE)
    # __________________________________________________________________________
    if not fs_check_access_dir('rw', args.path):
        return False
    # __________________________________________________________________________
    pid_file_path = os.path.join(tempfile.gettempdir(), os.path.basename(sys.argv[0]) + '.pid')
    if not pid_mk_file(pid_file_path):
        return False
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    # Collection of information
    # ------------------------------------------------------------------------------------------------------------------
    pg_version = psql_get_version(args.host, args.port, args.user)
    if pg_version is None:
        return False
    print(f"[II] Postgres server version: {pg_version}", flush=True)
    # __________________________________________________________________________
    pg_db_list = psql_get_databases(args.host, args.port, args.user)
    if pg_db_list is None:
        return False
    if _GLOBALS_NAME in pg_db_list:
        print(f"[EE] Database name cannot be: {_GLOBALS_NAME}", flush=True)
        return False
    pg_db_list = list(filter(lambda x: x not in args.exclude, pg_db_list))
    if not pg_db_list:
        print("[EE] Database list is empty", flush=True)
        return False
    # ==================================================================================================================
    # ==================================================================================================================
    # Start
    # ==================================================================================================================
    # WARNING: Don't use "return" in cycle
    tmp_backup_dir = os.path.join(args.path, f"{__START_DT.strftime('%Y.%m.%d_%H%M%S')}_tmp")
    good_backup_dir = os.path.join(args.path, f"{__START_DT.strftime('%Y.%m.%d_%H%M%S')}_good")
    error_backup_dir = os.path.join(args.path, f"{__START_DT.strftime('%Y.%m.%d_%H%M%S')}_error")
    if args.dry_run:
        print("[WW] DRY RUN MODE", flush=True)
    else:
        if not fs_mkdir(tmp_backup_dir):
            return False
    # ------------------------------------------------------------------------------------------------------------------
    # Globals
    # ------------------------------------------------------------------------------------------------------------------
    print(f"[..] Dumping globals: ...", flush=True)
    dst_path = os.path.join(tmp_backup_dir, f"{_GLOBALS_NAME}.sql")
    tmp_path = os.path.join(tmp_backup_dir, f"_tmp_{_GLOBALS_NAME}.sql")
    start_dt = datetime.datetime.now()
    if not pg_dump_globals(args.host, args.port, args.user, tmp_path, args.dry_run):
        main_return_value = False
    else:
        if not args.dry_run:
            duration = datetime.datetime.now() - start_dt
            if not fs_move(tmp_path, dst_path):
                main_return_value = False
            else:
                print("[OK] Successfully dumped", flush=True)
                print(f"\tpath: {dst_path}", flush=True)
                print(f"\tsize: {fs_sizeof_file(dst_path)}", flush=True)
                print(f"\tmd5: {fs_md5sum_file(dst_path)}", flush=True)
                print(f"\tduration: {duration}", flush=True)
                print(f"[--]", flush=True)
    # ------------------------------------------------------------------------------------------------------------------
    # Databases
    # ------------------------------------------------------------------------------------------------------------------
    for db in pg_db_list:
        print("[..] Dumping database: {} ...".format(db), flush=True)
        if args.njobs == 0:
            dst_path = os.path.join(tmp_backup_dir, f"{db}.pg_dump")
            tmp_path = os.path.join(tmp_backup_dir, f"_tmp_{db}.pg_dump")
        else:
            dst_path = os.path.join(tmp_backup_dir, f"{db}")
            tmp_path = os.path.join(tmp_backup_dir, f"_tmp_{db}")
        start_dt = datetime.datetime.now()
        if not pg_dump_database(args.host, args.port, args.user, db, tmp_path, args.compress, args.njobs, args.dry_run):
            main_return_value = False
        else:
            if not args.dry_run:
                duration = datetime.datetime.now() - start_dt
                if not fs_move(tmp_path, dst_path):
                    main_return_value = False
                else:
                    print("[OK] Successfully dumped", flush=True)
                    print(f"\tpath: {dst_path}", flush=True)
                    if args.njobs == 0:
                        print(f"\tsize: {fs_sizeof_file(dst_path)}", flush=True)
                        print(f"\tmd5: {fs_md5sum_file(dst_path)}", flush=True)
                    else:
                        print(f"\tsize: {fs_sizeof_dir(dst_path)}", flush=True)

                    print(f"\tduration: {duration}", flush=True)
                    print(f"[--]", flush=True)
    # ==================================================================================================================
    # ==================================================================================================================
    # End
    # ==================================================================================================================
    if not args.dry_run:
        dst_path = good_backup_dir if main_return_value else error_backup_dir
        if not fs_move(tmp_backup_dir, dst_path):
            main_return_value = False
        else:
            print(f"[{'OK' if main_return_value else 'EE'}] Done: {dst_path}", flush=True)
    # __________________________________________________________________________
    if not fs_rm_file(pid_file_path):
        main_return_value = False
    # __________________________________________________________________________
    return main_return_value


# ======================================================================================================================
# Functions
# ======================================================================================================================
def fs_check_access_dir(mode: str, *args) -> bool:
    return_value = True
    modes = {'ro': os.R_OK, 'rx': os.X_OK, 'rw': os.W_OK}
    for x in args:
        if os.path.exists(x):
            if os.path.isdir(x):
                if not os.access(x, modes[mode]):
                    print(f"[EE] Directory access denied: {x} ({mode})", flush=True)
                    return_value = False
            else:
                print(f"[EE] Is not directory: {x}", flush=True)
                return_value = False
        else:
            print(f"[EE] Directory does not exist: {x}", flush=True)
            return_value = False
    # __________________________________________________________________________
    return return_value


def fs_mkdir(path: str, recursive=False) -> bool:
    try:
        if os.path.exists(path):
            print(f"[..] Directory already exists: {os.path.abspath(path)}", flush=True)
            return True
        if recursive:
            os.makedirs(path)
        else:
            os.mkdir(path)
    except Exception as err:
        print(f"[!!] Exception: {type(err)}\n{''.join(traceback.format_exc(limit=1))}", flush=True)
        return False
    # __________________________________________________________________________
    print(f"[OK] Directory created: {os.path.abspath(path)}", flush=True)
    return True


def pid_mk_file(path):
    if os.path.exists(path):
        try:
            f = open(path, 'r')
            pid = int(f.readline().strip())
            f.close()
        except ValueError as err:
            print("[EE] Pid file already exist. Incorrect file contents", flush=True)
            return False
        except Exception as err:
            print(f"[!!] Exception: {type(err)}\n{''.join(traceback.format_exc(limit=1))}", flush=True)
            return False
        # __________________________________________________________________________
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            print(f"[WW] Pid file already exist. Process does not exist: {pid}", flush=True)
        except PermissionError:
            print(f"[EE] Pid file already exist. Process already running: {pid}", flush=True)
            return False
        except Exception as err:
            print(f"[!!] Exception: {type(err)}\n{''.join(traceback.format_exc(limit=1))}", flush=True)
            return False
        else:
            print(f"[EE] Pid file already exist. Process already running: {pid}", flush=True)
            return False
    # __________________________________________________________________________
    try:
        f = open(path, 'w')
        f.write('{}\n'.format(os.getpid()))
        f.close()
    except Exception as err:
        print(f"[!!] Exception: {type(err)}\n{''.join(traceback.format_exc(limit=1))}", flush=True)
        return False
    # __________________________________________________________________________
    return True


def fs_rm_file(path: str) -> bool:
    try:
        os.remove(path)
    except Exception as err:
        print(f"[!!] Exception: {type(err)}\n{''.join(traceback.format_exc(limit=1))}", flush=True)
        return False
    # __________________________________________________________________________
    return True


def shell_exec(cmd: str, shell: str = "/bin/bash") -> (int, str):
    child = subprocess.Popen(cmd,
                             shell=True,
                             executable=shell,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT,
                             stdin=subprocess.PIPE)
    stdout = child.communicate()[0]
    returncode = child.returncode
    # __________________________________________________________________________
    return returncode, stdout.decode("utf-8").strip()


def fs_sizeof_file(path: str, delimiter: str = ' ') -> str:
    # noinspection PyBroadException
    try:
        size = os.path.getsize(path)
        for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return "{0:0.2f}{1}{2}".format(size, delimiter, x)
            size /= 1024.0
    except Exception as err:
        print(f"[!!] Exception: {type(err)}\n{''.join(traceback.format_exc(limit=1))}", flush=True)
        return ""


def fs_sizeof_dir(path: str, delimiter: str = ' ') -> str:
    # noinspection PyBroadException
    try:
        size = sum(d.stat().st_size for d in os.scandir(path) if d.is_file())
        for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return "{0:0.2f}{1}{2}".format(size, delimiter, x)
            size /= 1024.0
    except Exception as err:
        print(f"[!!] Exception: {type(err)}\n{''.join(traceback.format_exc(limit=1))}", flush=True)
        return ""


def fs_md5sum_file(path: str) -> str:
    md5 = hashlib.md5()
    # noinspection PyBroadException
    try:
        with open(path, 'rb') as f:
            while chunk := f.read(1048576):
                md5.update(chunk)
            return md5.hexdigest()
    except Exception as err:
        print(f"[!!] Exception: {type(err)}\n{''.join(traceback.format_exc(limit=1))}", flush=True)
        return ""


def fs_move(src_path: str, dst_path: str) -> bool:
    if os.path.exists(dst_path):
        print("f[EE] Destination already exists: {}".format(dst_path), flush=True)
        return False
    try:
        os.replace(src_path, dst_path)
    except Exception as err:
        print(f"[!!] Exception: {type(err)}\n{''.join(traceback.format_exc(limit=1))}", flush=True)
        return False
    # __________________________________________________________________________
    return True


# ======================================================================================================================
# PG Functions
# ======================================================================================================================
def psql_get_version(host: str, port: int, user: str) -> Union[None, str]:
    sql = "SHOW server_version;"
    cmd = '''psql -h "{}" -p "{}" -U "{}" -tA -c "{}"'''.format(host, port, user, sql)
    rc, rd = shell_exec(cmd)
    if rc != 0 or not rd:
        print("[EE] Shell command executed. Exit code: {0}\n{1}\n{2}\n{1}\n{3}\n{1}".format(
            rc, "-  " * 33 + "-", cmd, rd), flush=True)
        return None
    # __________________________________________________________________________
    return rd


def psql_get_databases(host: str, port: int, user: str) -> Union[None, list]:
    re_db_name = re.compile(r'^\s*(\w+)\s*\|')
    cmd = '''psql -h "{}" -p "{}" -U "{}" -tl'''.format(host, port, user)
    rc, rd = shell_exec(cmd)
    if rc != 0 or not rd:
        print("[EE] Shell command executed. Exit code: {0}\n{1}\n{2}\n{1}\n{3}\n{1}".format(
            rc, "-  " * 33 + "-", cmd, rd), flush=True)
        return None
    # __________________________________________________________________________
    _tmp = rd.split('\n')
    _tmp = filter(lambda x: re_db_name.search(x), _tmp)
    _tmp = map(lambda x: re_db_name.search(x).group(1), _tmp)
    # __________________________________________________________________________
    return list(_tmp)


def pg_dump_globals(host: str, port: int, user: str, path: str, dry_run: bool = False) -> bool:
    cmd = '''pg_dumpall -h "{}" -p "{}" -U "{}" --globals-only -f "{}"'''.format(host, port, user, path)
    if dry_run:
        print("\t{}".format(cmd), flush=True)
        return True
    # __________________________________________________________________________
    rc, rd = shell_exec(cmd)
    if rc != 0:
        print("[EE] Shell command executed. Exit code: {0}\n{1}\n{2}\n{1}\n{3}\n{1}".format(
            rc, "-  " * 33 + "-", cmd, rd), flush=True)
        return False
    # __________________________________________________________________________
    return True


def pg_dump_database(host: str, port: int, user: str, dbname: str, path: str, compress: str = "", njobs: int = 0,
                     dry_run: bool = False) -> bool:
    cmd = '''pg_dump -h "{}" -p "{}" -U "{}" -f "{}"'''.format(host, port, user, path)
    if compress:
        cmd += ''' --compress="{}"'''.format(compress)
    if njobs == 0:
        cmd += ''' -Fc "{}"'''.format(dbname)
    else:
        cmd += ''' -j "{}" -Fd "{}" '''.format(njobs, dbname)
    if dry_run:
        print("\t{}".format(cmd), flush=True)
        return True
    # __________________________________________________________________________
    rc, rd = shell_exec(cmd)
    if rc != 0:
        print("[EE] Shell command executed. Exit code: {0}\n{1}\n{2}\n{1}\n{3}\n{1}".format(
            rc, "-  " * 33 + "-", cmd, rd), flush=True)
        return False
    # __________________________________________________________________________
    return True


# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
if __name__ == '__main__':
    print("{0}\n{1} PID={2} PPID={3} HOST={4} NAME={5}\n{0}".format(
        "-" * 100, __START_DT, os.getpid(), os.getppid(), __HOSTNAME, os.path.basename(sys.argv[0])), flush=True)
    exit_status = main()
    print("{0}\n{1} PID={2} DURATION={3} RETURN={4}\n{0}".format(
        "-" * 100, datetime.datetime.now(), os.getpid(), datetime.datetime.now() - __START_DT, exit_status), flush=True)
    # __________________________________________________________________________
    sys.exit(not exit_status)  # Compatible return code
