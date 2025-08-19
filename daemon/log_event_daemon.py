#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------------------------------------------------
import argparse
import datetime
import os
import queue
import re
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import traceback
from time import sleep
from typing import Union

__START_DT = datetime.datetime.now()
__HOSTNAME = socket.getfqdn()
__GLOBAL = {'stop': False, 'return': True}

LogQueue = queue.Queue(4096)
CmdQueue = queue.Queue(1024)

# demo
re_log_entry_1 = re.compile(r'^done$')
action_1_cmd = '''echo "$(date) ok" > /tmp/action.log'''
action_1_repeat_interval = 60  # The time to wait before repeat an action, in seconds.
action_1_buffer = {'last': datetime.datetime.min}


def main():
    # __________________________________________________________________________
    # command-line options, arguments
    try:
        parser = argparse.ArgumentParser(
            description='Continuously watches a single file and reacts to regex-matched lines.')
        parser.add_argument('file', action='store', type=str,
                            metavar="<FILE>", help="File to watch for new lines")
        parser.add_argument('-n', '--dry-run', action='store_true',
                            help="testing mode with no changes made")
        args = parser.parse_args()  # <class 'argparse.Namespace'>
    except SystemExit:
        return False
    # ==================================================================================================================
    # ==================================================================================================================
    # Init threads
    # ==================================================================================================================
    thread_reader = ThreadReader(name="ThreadReader", dry_run=args.dry_run)
    if not thread_reader.trg_init:
        print(f"[EE] Failed init thread: {thread_reader.name}", flush=True)
        return False
    # __________________________________________________________________________
    thread_commander = ThreadCommander(name="ThreadCommander", dry_run=args.dry_run)
    if not thread_commander.trg_init:
        print(f"[EE] Failed init thread: {thread_commander.name}", flush=True)
        return False
    # ==================================================================================================================
    # ==================================================================================================================
    # Open subprocess
    # ==================================================================================================================
    if args.dry_run:
        # ______________________________________________________________________
        # PID
        pid_file_path = None
        # ______________________________________________________________________
        # cat
        print("[WW] DRY RUN MODE", flush=True)
        cmd = f'''export LC_ALL=""; export LANG="en_US.UTF-8"; cat "{args.file}"'''
        child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        print(f"[..] Running subprocess with PID: {child.pid}", flush=True)
    else:
        # ______________________________________________________________________
        # PID
        pid_file_path = os.path.join(tempfile.gettempdir(), os.path.basename(sys.argv[0]) + '.pid')
        if not pid_mk_file(pid_file_path):
            return False
        # ______________________________________________________________________
        # tail
        cmd = f'''export LC_ALL=""; export LANG="en_US.UTF-8"; tail -n0 -F "{args.file}"'''
        child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        print(f"[..] Running subprocess with PID: {child.pid}", flush=True)
        sleep(0.5)
        # child.poll() - Check if child process has terminated. Set and return returncode attribute.
        if child.poll() is not None:
            print("$ {0}\n{1}".format(cmd, child.communicate()[0]), flush=True)
            print(f"[EE] Subprocess unexpectedly closed PID: {child.pid} RETURN: {child.returncode}", flush=True)
            return False
    # ==================================================================================================================
    # ==================================================================================================================
    # Start threads
    # ==================================================================================================================
    signal.signal(signal.SIGINT, signal_handler_sigint)
    signal.signal(signal.SIGTERM, signal_handler_sigint)
    #
    thread_reader.start()
    thread_commander.start()
    #
    readline_count = 0
    while True:
        readline_count += 1
        line = child.stdout.readline()  # <class 'bytes'>
        # print(readline_count, __GLOBAL['stop'], child.poll(), line)  #### TEST
        if __GLOBAL['stop']:
            # __________________________________________________________________
            # Terminate a child process, first kill all its descendants + foolproof protection !!!
            children = list(filter(lambda x: x > 1024, ps_get_children(child.pid, recursive=True)))
            children.sort(reverse=True)
            if len(children) > 2:
                print(f"[EE] Too many children at PID: {child.pid} for kill: {','.join([str(x) for x in children])}",
                      flush=True)
            else:
                for pid in children:
                    ps_kill(pid, signal.SIGINT)
            child.kill()
            sleep(0.1)
            child.poll()
            print(f"[..] Subprocess closed PID: {child.pid} RETURN: {child.returncode}", flush=True)
            break
        if child.poll() is not None and not line:
            if not args.dry_run:
                print(f"[EE] Subprocess unexpectedly closed PID: {child.pid} RETURN: {child.returncode}", flush=True)
                __GLOBAL['return'] = False
            break
        # ______________________________________________________________________
        # fast filter !!!
        line = line.rstrip()  # <class 'bytes'>
        if line:
            LogQueue.put(line)
        # sleep(0.1)  #### TEST
    # ==================================================================================================================
    # ==================================================================================================================
    # Stop threads
    # ==================================================================================================================
    thread_reader.stop()
    thread_commander.stop()
    thread_reader.join()
    thread_commander.join()
    # __________________________________________________________________________
    if pid_file_path is not None:
        if not fs_rm_file(pid_file_path):
            __GLOBAL['return'] = False
    # __________________________________________________________________________
    return __GLOBAL['return']


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
        # ______________________________________________________________________
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


def ps_kill(pid: int, signum: int = 9):
    try:
        os.kill(pid, signum)
    except OSError:
        return False
    else:
        return True


def ps_get_pid_list():
    try:
        pids = [int(x) for x in os.listdir('/proc') if x.isdigit()]
        return pids
    except Exception as err:
        print("[!!] Exception :: {}\n{}".format(err, "".join(traceback.format_exc())), flush=True)
        return None


def ps_get_ppid(pid: int) -> Union[None, int]:
    try:
        f = open("/proc/{0}/status".format(pid))
        for line in f:
            if line.startswith("PPid:"):
                f.close()
                return int(line.split()[1])
        print(f"[..] PPid not found PID: {pid}", flush=True)
        f.close()
        return None
    except Exception as err:
        print("[!!] Exception :: {}\n{}".format(err, "".join(traceback.format_exc())), flush=True)
        return None


def ps_get_children(pid: int, recursive=False):
    process_list = [(proc_pid, ps_get_ppid(proc_pid)) for proc_pid in ps_get_pid_list()]
    tmp = filter(lambda x: x[1] == pid, process_list)
    result = [x[0] for x in tmp]
    if recursive:
        recursive_result = []
        for x in result:
            for y in ps_get_children(x, recursive=True):
                recursive_result.append(y)
        return result + recursive_result
    else:
        return result


def human_readable_signal(signum: int) -> Union[int, str]:
    for s, n in signal.__dict__.items():
        if n == signum and isinstance(n, signal.Signals):
            return s
    else:
        return signum


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


# ======================================================================================================================
# Classes
# ======================================================================================================================
class ThreadCommander(threading.Thread):
    def __init__(self, name: str, dry_run: bool = False):
        threading.Thread.__init__(self)
        self.name = name
        self.dry_run = dry_run
        self.trg_stop = False
        self.trg_init = False
        # ______________________________________________________________________
        self.trg_init = True

    def run(self):
        print(f"[..] Thread starting: {self.name} ...", flush=True)
        # heartbit = 1
        while True:
            # heartbit += 1
            # print(f"::{self.name}:: Heartbit: {heartbit}", flush=True)  #### TEST
            # __________________________________________________________________
            if self.trg_stop:
                return
            # __________________________________________________________________
            try:
                cmd = CmdQueue.get(timeout=1)  # Blocking if timeout not set
                CmdQueue.task_done()
            except queue.Empty:
                continue
            # __________________________________________________________________
            start_dt = datetime.datetime.now()
            rc, rd = shell_exec(cmd, dry_run=self.dry_run)
            duration = datetime.datetime.now() - start_dt
            if rc != 0:
                print("[EE] Shell EXIT: {0} DURATION: {1}\n{2}\n{3}\n{4}\n{2}".format(
                    rc, duration, "-  " * 33 + "-", cmd, rd), flush=True)
            else:
                print("[OK] Shell EXIT: {0} DURATION: {1}\n{2}\n{3}\n{4}\n{2}".format(
                    rc, duration, "-  " * 33 + "-", cmd, rd), flush=True)
            # __________________________________________________________________
            sleep(1)

    def stop(self):
        print(f"[..] Thread stopping: {self.name} ...", flush=True)
        self.trg_stop = True


class ThreadReader(threading.Thread):
    def __init__(self, name: str, dry_run: bool = False):
        threading.Thread.__init__(self)
        self.name = name
        self.dry_run = dry_run
        self.trg_stop = False
        self.trg_init = False
        # ______________________________________________________________________
        self.trg_init = True

    def run(self):
        print(f"[..] Thread starting: {self.name} ...", flush=True)
        # heartbit = 0
        while True:
            # heartbit += 1
            # print(f"::{self.name}:: Heartbit: {heartbit}", flush=True)  #### TEST
            # __________________________________________________________________
            if self.trg_stop:
                return
            # __________________________________________________________________
            try:
                line = LogQueue.get(timeout=1)  # Blocking if timeout not set
                LogQueue.task_done()
            except queue.Empty:
                line = None
            # __________________________________________________________________
            if line is not None:
                # Processing
                processing_action_1(line.decode('utf-8'))

    def stop(self):
        print(f"[..] Thread stopping: {self.name} ...", flush=True)
        self.trg_stop = True


# ======================================================================================================================
# Processing
# ======================================================================================================================
def processing_action_1(line: str):
    # __________________________________________________________________________
    if not re_log_entry_1.search(line):
        # print(f"IGNORE: {line}")  #### TEST
        return
    # __________________________________________________________________________
    if (datetime.datetime.now() - action_1_buffer['last']).seconds < action_1_repeat_interval:
        # print(f"TIMEOUT: action_1")  #### TEST
        return
    # __________________________________________________________________________
    # print("TODO: action_1")  #### TEST
    CmdQueue.put(action_1_cmd)
    action_1_buffer['last'] = datetime.datetime.now()
    # __________________________________________________________________________
    return


# ======================================================================================================================
# Signal Handlers
# ======================================================================================================================
# noinspection PyShadowingNames
def signal_handler_sigint(signum, frame):
    print(f"\n[..] Received signal: {human_readable_signal(signum)}", flush=True)
    __GLOBAL['stop']: True


# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
if __name__ == '__main__':
    print("[  ] {0}\n[..] {1} PID={2} PPID={3} HOST={4} NAME={4}\n[  ] {0}".format(
        "-" * 95, __START_DT, os.getpid(), os.getppid(), __HOSTNAME, os.path.basename(sys.argv[0])), flush=True)
    exit_status = main()
    print("[  ] {0}\n[..] {1} PID={2} DURATION={3} RETURN={4}\n[  ] {0}".format(
        "-" * 95, datetime.datetime.now(), os.getpid(), datetime.datetime.now() - __START_DT, exit_status), flush=True)
    # __________________________________________________________________________
    sys.exit(not exit_status)  # Compatible return code
