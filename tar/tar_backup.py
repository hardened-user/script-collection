#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------------------------------------------------
import argparse
import datetime
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import traceback
from collections.abc import Iterable
from typing import Union

import ruamel.yaml.comments
from ruamel.yaml import YAML

_DEFAULT_CONFIG_FILE = "tar_backup.yaml"
_DATE_TIME_FORMAT = r'%Y.%m.%d_%H%M%S'
_DATE_TIME_REGEXP = r'\d{4}\.\d{2}\.\d{2}_\d{6}'

__START_DT = datetime.datetime.now()
__HOSTNAME = socket.getfqdn()


def main():
    main_return_value = True
    re_simple_str = re.compile(r"^([\w\-]+)$")
    # __________________________________________________________________________
    # command-line options, arguments
    try:
        parser = argparse.ArgumentParser(description='Backup with TAR util. Supported differential backup.')
        parser.add_argument('-c', '--config', action='store', type=str,
                            help=f"config yaml file path (default: {_DEFAULT_CONFIG_FILE})")
        parser.add_argument('-t', '--task', action='append', type=str,
                            help="task (default: all tasks)")
        parser.add_argument('-n', '--dry-run', action='store_true',
                            help="testing mode with no changes made")
        args = parser.parse_args()  # <class 'argparse.Namespace'>
    except SystemExit:
        return False
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    # Validate
    # ------------------------------------------------------------------------------------------------------------------
    if args.config is None:
        args.config = os.path.join(os.path.dirname(__file__), _DEFAULT_CONFIG_FILE)
    # __________________________________________________________________________
    # Configuration
    config_data_yaml = yaml_load_file(args.config)  # <class 'ruamel.yaml.comments.CommentedMap'>
    if config_data_yaml is None:
        return False
    #
    config_tasks_yaml = config_data_yaml.get('tasks')  # <class 'ruamel.yaml.comments.CommentedSeq'>
    if not isinstance(config_tasks_yaml, Iterable):
        print("[EE] Invalid configuration file", flush=True)
        return False
    #
    config_tasks_names = filter(lambda a: a.get('name') is not None, config_tasks_yaml)
    config_tasks_names = list(map(lambda a: a.get('name'), config_tasks_names))
    if len(config_tasks_names) != len(set(config_tasks_names)):
        print("[EE] Duplicate tasks found", flush=True)
        return False
    #
    config_exclude_tag = config_data_yaml.get('exclude_tag')
    # __________________________________________________________________________
    # PID
    pid_file_path = os.path.join(tempfile.gettempdir(), os.path.basename(sys.argv[0]) + '.pid')
    if not pid_mk_file(pid_file_path):
        return False
    # ==================================================================================================================
    # ==================================================================================================================
    # Start
    # ==================================================================================================================
    task_counter = 0
    if args.dry_run:
        print("[WW] DRY RUN MODE", flush=True)
    for task in config_tasks_yaml:
        config = {
            'name': None,  # *require
            'source': None,  # *require
            'store_dir': None,  # *require
            'store_max': 3,  # default
            'differential': 0,  # default
            'exclude_tag': config_exclude_tag,
            'enabled': True,  # default
            'exclude': [],  # default
        }
        # ______________________________________________________________________
        for x in config.keys():
            try:
                if task.get(x) is None:
                    continue
                config[x] = task.get(x)
            except KeyError:
                pass
            except Exception as err:
                print(f"[!!] Exception: {type(err)}\n{''.join(traceback.format_exc(limit=1))}")
                main_return_value = False
                continue
        # --------------------------------------------------------------------------------------------------------------
        # Checking
        # --------------------------------------------------------------------------------------------------------------
        # name
        if not isinstance(config['name'], str) or not re_simple_str.search(config['name']):
            print(f"[EE] Invalid task name: {config['name']}", flush=True)
            main_return_value = False
            continue
        if isinstance(args.task, Iterable):
            if config['name'] not in args.task:
                continue
        print("[  ]", flush=True)
        print("[--] {0} {1}".format(config['name'], '-' * (94 - len(config['name']))), flush=True)
        # ______________________________________________________________________
        # enabled
        if not config['enabled']:
            print(f"[..] Task disabled", flush=True)
            continue
        # ______________________________________________________________________
        # source
        if not config['source'] or not isinstance(config['source'], str):
            print(f"[EE] Invalid task source: {config['source']}", flush=True)
            main_return_value = False
            continue
        # ______________________________________________________________________
        # store_dir
        if not config['store_dir'] or not isinstance(config['store_dir'], str):
            print(f"[EE] Invalid task store_dir: {config['store_dir']}", flush=True)
            main_return_value = False
            continue
        # ______________________________________________________________________
        # store_max
        if not isinstance(config['store_max'], int) or config['store_max'] < 1:
            print(f"[EE] Invalid task store_max: {config['store_max']}", flush=True)
            main_return_value = False
            continue
        # ______________________________________________________________________
        # differential
        if not isinstance(config['differential'], int) or config['differential'] < 0:
            print(f"[EE] Invalid task differential: {config['differential']}", flush=True)
            main_return_value = False
            continue
        # ______________________________________________________________________
        # exclude
        if not isinstance(config['exclude'], list):
            print(f"[EE] Invalid task exclude: {config['exclude']}", flush=True)
            main_return_value = False
            continue
        config['exclude'] = list(filter(lambda a: a, config['exclude']))
        # --------------------------------------------------------------------------------------------------------------
        # Archiving
        # --------------------------------------------------------------------------------------------------------------
        task_counter += 1
        if config['differential']:
            if not tar_differential(config, args.dry_run):
                print("[EE] Archiving failed", flush=True)
                main_return_value = False
        else:
            if not tar_standard(config, args.dry_run):
                print("[EE] Archiving failed", flush=True)
                main_return_value = False
        # --------------------------------------------------------------------------------------------------------------
        # Rotation
        # --------------------------------------------------------------------------------------------------------------
        if not rotate_processing(config, args.dry_run):
            print("[EE] Rotation failed", flush=True)
            main_return_value = False
    # ==================================================================================================================
    # ==================================================================================================================
    # End
    # ==================================================================================================================
    if task_counter == 0:
        print("[EE] Nothing to do", flush=True)
        main_return_value = False
    # __________________________________________________________________________
    if not fs_rm_file(pid_file_path):
        main_return_value = False
    # __________________________________________________________________________
    return main_return_value


# ======================================================================================================================
# Functions
# ======================================================================================================================
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


def fs_rm_file(path: str, dry_run: bool = False) -> bool:
    if dry_run:
        print(f"$ rm {path}", flush=True)
        return True
    try:
        os.remove(path)
    except Exception as err:
        print(f"[!!] Exception: {type(err)}\n{''.join(traceback.format_exc(limit=1))}", flush=True)
        return False
    # __________________________________________________________________________
    return True


def fs_move(src: str, dst: str, dry_run: bool = False) -> bool:
    if dry_run:
        print(f"$ mv {src} {dst}", flush=True)
        return True
    if os.path.exists(dst):
        print(f"[EE] Destination already exists: {dst}", flush=True)
        return False
    try:
        os.replace(src, dst)
    except Exception as err:
        print(f"[!!] Exception: {type(err)}\n{''.join(traceback.format_exc(limit=1))}", flush=True)
        return False
    # __________________________________________________________________________
    return True


def fs_cp_file(src: str, dst: str, dry_run: bool = False) -> bool:
    if dry_run:
        print(f"$ cp {src} {dst}", flush=True)
        return True
    try:
        shutil.copy2(src, dst)
    except Exception as err:
        print(f"[!!] Exception: {type(err)}\n{''.join(traceback.format_exc(limit=1))}", flush=True)
        return False
    # __________________________________________________________________________
    return True


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


def shell_exec(cmd: str, shell: str = "/bin/bash", dry_run: bool = False) -> (int, str):
    if dry_run:
        print(f"$ {cmd}", flush=True)
        return 0, ""
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


def yaml_load_file(path: str) -> Union[None, ruamel.yaml.comments.CommentedMap]:
    yaml = YAML()
    try:
        with open(path, 'rt', encoding='utf-8') as f:
            data = yaml.load(f)
    except Exception as err:
        print(f"[!!] Exception: {type(err)}\n{''.join(traceback.format_exc(limit=1))}")
        return None
    # __________________________________________________________________________
    if data is None:
        return ruamel.yaml.comments.CommentedMap()
    # __________________________________________________________________________
    return data  # <class 'ruamel.yaml.comments.CommentedMap'>


# ======================================================================================================================
# TAR Functions
# ======================================================================================================================
def tar_standard(config: dict, dry_run: bool = False):
    print("[..] Standard archiving", flush=True)
    print("[..] Creating FULL ...")
    now_dt_str = __START_DT.strftime(_DATE_TIME_FORMAT)
    arch_file_type = "full"
    arch_dst_name = f"{now_dt_str}.{config['name']}.{arch_file_type}.tar.gz"
    arch_tmp_name = f"{arch_dst_name}_tmp"
    arch_dst_path = os.path.join(config['store_dir'], arch_dst_name)
    arch_tmp_path = os.path.join(config['store_dir'], arch_tmp_name)
    # __________________________________________________________________________
    if os.path.exists(arch_dst_path):
        print(f"[EE] File already exists: {arch_dst_path}", flush=True)
        return False
    # __________________________________________________________________________
    cmd = '''cd / && tar czpf "{0}"'''.format(arch_tmp_path)
    # add --exclude-tag
    if config['exclude_tag']:
        cmd += ''' \\\n  --exclude-tag="{0}"'''.format(config['exclude_tag'])
    # add --exclude
    for x in config['exclude']:
        cmd += ''' \\\n  --exclude="{0}"'''.format(x)
    # append source directory at last
    cmd += ''' \\\n  "{0}"'''.format(config['source'])
    # __________________________________________________________________________
    start_dt = datetime.datetime.now()
    rc, rd = shell_exec(cmd, dry_run=dry_run)
    duration = datetime.datetime.now() - start_dt
    if rc != 0:
        print("[EE] Shell command executed. Exit code: {0}\n{1}\n{2}\n{1}\n{3}\n{1}".format(
            rc, "-  " * 33 + "-", cmd, rd), flush=True)
        return False
    if not fs_move(arch_tmp_path, arch_dst_path, dry_run=dry_run):
        return False
    if not dry_run:
        print(f"[OK] {arch_dst_path}", flush=True)
        print(f"\tsize: {fs_sizeof_file(arch_dst_path)}", flush=True)
        print(f"\tduration: {duration}", flush=True)
    # __________________________________________________________________________
    return True


def tar_differential(config: dict, dry_run: bool = False):
    print("[..] Differential archiving", flush=True)
    now_dt_str = __START_DT.strftime(_DATE_TIME_FORMAT)
    # __________________________________________________________________________
    # find last full archive
    re_full = re.compile(rf"^(?P<dt>{_DATE_TIME_REGEXP})\.(?P<name>[\w\-]+)\.full\.tar\.")
    try:
        f_list = os.listdir(config['store_dir'])
        f_list = filter(lambda x: os.path.isfile(os.path.join(config['store_dir'], x)), f_list)
        f_list = filter(lambda x: (match := re_full.search(x)) and match.group('name') == config['name'], f_list)
        f_list = list(f_list)
    except Exception as err:
        print(f"[!!] Exception: {type(err)}\n{''.join(traceback.format_exc(limit=1))}")
        return False
    #
    last_arch_name = ""
    last_arch_path = ""
    last_arch_dt_str = ""
    last_snar_path = ""
    last_arch_age = None
    if f_list:
        last_arch_name = f_list[-1]
        last_arch_path = os.path.join(config['store_dir'], last_arch_name)
        last_arch_dt_str = f"{re_full.search(last_arch_name).group('dt')}"
        last_snar_name = f"{last_arch_dt_str}.{config['name']}.full.snar"
        last_snar_path = os.path.join(config['store_dir'], last_snar_name)
        try:
            last_arch_age = __START_DT - datetime.datetime.strptime(last_arch_dt_str, _DATE_TIME_FORMAT)
        except Exception as err:
            print(f"[!!] Exception: {type(err)}\n{''.join(traceback.format_exc(limit=1))}")
            return False
    # __________________________________________________________________________
    if last_arch_name and \
            os.path.exists(last_arch_path) and os.path.exists(last_snar_path) and \
            last_arch_age.days <= config['differential']:
        print("[..] Creating DIFF ...")
        arch_file_type = "diff"
        arch_dst_name = f"{now_dt_str}.{config['name']}.{arch_file_type}.{last_arch_dt_str}.tar.gz"
        arch_tmp_name = f"{arch_dst_name}_tmp"
        arch_dst_path = os.path.join(config['store_dir'], arch_dst_name)
        arch_tmp_path = os.path.join(config['store_dir'], arch_tmp_name)
        #
        snar_dst_name = f"{now_dt_str}.{config['name']}.{arch_file_type}.{last_arch_dt_str}.snar"
        snar_tmp_name = f"{snar_dst_name}_tmp"
        snar_dst_path = os.path.join(config['store_dir'], snar_dst_name)
        snar_tmp_path = os.path.join(config['store_dir'], snar_tmp_name)
        #
        if os.path.exists(arch_dst_path):
            print(f"[EE] File already exists: {arch_dst_path}", flush=True)
            return False
        #
        if not fs_cp_file(last_snar_path, snar_tmp_path, dry_run=dry_run):
            return False
    # __________________________________________________________________________
    else:
        print("[..] Creating FULL ...")
        arch_file_type = "full"
        arch_dst_name = f"{now_dt_str}.{config['name']}.{arch_file_type}.tar.gz"
        arch_tmp_name = f"{arch_dst_name}_tmp"
        arch_dst_path = os.path.join(config['store_dir'], arch_dst_name)
        arch_tmp_path = os.path.join(config['store_dir'], arch_tmp_name)
        #
        snar_dst_name = f"{now_dt_str}.{config['name']}.{arch_file_type}.snar"
        snar_tmp_name = f"{snar_dst_name}_tmp"
        snar_dst_path = os.path.join(config['store_dir'], snar_dst_name)
        snar_tmp_path = os.path.join(config['store_dir'], snar_tmp_name)
        #
        if os.path.exists(arch_dst_path):
            print(f"[EE] File already exists: {arch_dst_path}", flush=True)
            return False
        #
        # NOTE: Remove if current snapshot exists
        if os.path.exists(snar_tmp_path):
            print(f"[WW] Delete unexpected snapshot file: {snar_tmp_path}", flush=True)
            if not fs_rm_file(snar_tmp_path, dry_run=dry_run):
                return False
    # __________________________________________________________________________
    cmd = '''cd / && tar czpf "{0}"'''.format(arch_tmp_path)
    # add --exclude-tag
    if config['exclude_tag']:
        cmd += ''' \\\n  --exclude-tag="{0}"'''.format(config['exclude_tag'])
    # add --exclude
    for x in config['exclude']:
        cmd += ''' \\\n  --exclude="{0}"'''.format(x)
    # add --listed-incremental
    if snar_tmp_path:
        cmd += ''' \\\n  --listed-incremental="{0}"'''.format(snar_tmp_path)
    # append source directory at last
    cmd += ''' \\\n  "{0}"'''.format(config['source'])
    # __________________________________________________________________________
    start_dt = datetime.datetime.now()
    rc, rd = shell_exec(cmd, dry_run=dry_run)
    duration = datetime.datetime.now() - start_dt
    if rc != 0:
        print("[EE] Shell command executed. Exit code: {0}\n{1}\n{2}\n{1}\n{3}\n{1}".format(
            rc, "-  " * 33 + "-", cmd, rd), flush=True)
        return False
    if not fs_move(arch_tmp_path, arch_dst_path, dry_run=dry_run):
        return False
    if not fs_move(snar_tmp_path, snar_dst_path, dry_run=dry_run):
        return False
    if not dry_run:
        print(f"[OK] {arch_dst_path}", flush=True)
        print(f"\tsize: {fs_sizeof_file(arch_dst_path)}", flush=True)
        print(f"\tduration: {duration}", flush=True)
        print(f"[OK] {snar_dst_path}", flush=True)
        print(f"\tsize: {fs_sizeof_file(snar_dst_path)}", flush=True)
    # __________________________________________________________________________
    return True


def rotate_processing(config: dict, dry_run: bool = False):
    print("[..] Rotation ...", flush=True)
    return_value = True
    re_file = re.compile(rf"^(?P<dt>{_DATE_TIME_REGEXP})\.(?P<name>[\w\-]+)\.(full|diff)\.")
    # __________________________________________________________________________
    if not os.path.exists(config['store_dir']):
        print(f"[EE] Directory does not exist: {config['store_dir']}", flush=True)
        return False
    # __________________________________________________________________________
    try:
        find_list = os.listdir(config['store_dir'])
        find_list = filter(lambda x: os.path.isfile(os.path.join(config['store_dir'], x)), find_list)
        find_list = map(lambda x: re_file.search(x), find_list)
        find_list = filter(lambda x: x and x.group('name') == config['name'], find_list)
        find_list = list(find_list)
        expired_date_list = sorted(set(map(lambda x: x.group('dt'), find_list)), reverse=True)[config['store_max']:]
        delete_list = filter(lambda x: x.group('dt') in expired_date_list, find_list)
        delete_list = list(delete_list)
    except Exception as err:
        print(f"[!!] Exception: {type(err)}\n{''.join(traceback.format_exc(limit=1))}")
        return False
    # __________________________________________________________________________
    print(f"[..] Deleting {len(delete_list)} of {len(find_list)}", flush=True)
    # __________________________________________________________________________
    # Deleting files
    for f in delete_list:
        print(f"\t{os.path.join(config['store_dir'], f.string)}", flush=True)
        if not fs_rm_file(str(os.path.join(config['store_dir'], f.string)), dry_run=dry_run):
            return_value = False
    # __________________________________________________________________________
    return return_value


# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
if __name__ == '__main__':
    print("[  ] {0}\n[..] {1} PID={2} PPID={3} HOST={4} NAME={4}\n[  ] {0}".format(
        "-" * 95, __START_DT, os.getpid(), os.getppid(), __HOSTNAME, os.path.basename(sys.argv[0])), flush=True)
    exit_status = main()
    print("[  ] {0}\n[..] {1} PID={2} DURATION={3} RETURN={4}\n[  ] {0}".format(
        "-" * 95, datetime.datetime.now(), os.getpid(), datetime.datetime.now() - __START_DT, exit_status), flush=True)
    # __________________________________________________________________________
    sys.exit(not exit_status)  # Compatible return code
