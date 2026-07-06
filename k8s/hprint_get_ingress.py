#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------------------------------------------------
import argparse
import sys
import traceback

from ruamel.yaml import YAML


def main():
    # __________________________________________________________________________
    # command-line options, arguments
    try:
        parser = argparse.ArgumentParser(
            description='Print Kubernetes Ingress resources in human-readable table format.',
            epilog='example:\n\tkubectl get ingress --all-namespaces -o yaml | %(prog)s',
            formatter_class=argparse.RawDescriptionHelpFormatter
        )
        # noinspection PyUnusedLocal
        args = parser.parse_args()  # <class 'argparse.Namespace'>
    except SystemExit:
        return False
    # __________________________________________________________________________
    yaml = YAML()
    yaml.indent(mapping=2, sequence=4, offset=2)
    buffer: dict[str, dict[str, dict[str, list[str]]]] = dict()
    ljust = {
        'ns': 0,
        'name': 0,
        'host': 0,
        'path': 0
    }
    try:
        ingress_yaml = yaml.load(stream=sys.stdin)
        items = ingress_yaml['items']
    except Exception as err:
        print(f"[!!] Exception: {type(err)}\n{''.join(traceback.format_exc(limit=1))}", flush=True)
        return False
    for item in items:
        ns = item['metadata']['namespace']
        buffer.setdefault(ns, dict())
        if len(ns) > ljust.get('ns', 0):
            ljust['ns'] = len(ns)
        #
        name = item['metadata']['name']
        buffer[ns].setdefault(name, dict())
        if len(name) > ljust.get('name', 0):
            ljust['name'] = len(name)
        #
        for rule in item['spec']['rules']:
            host = rule['host']
            buffer[ns][name].setdefault(host, list())
            if len(host) > ljust.get('host', 0):
                ljust['host'] = len(host)
            #
            for path in rule['http']['paths']:
                path = path.get('path', '')
                buffer[ns][name][host].append(path)
                if len(path) > ljust.get('path', 0):
                    ljust['path'] = len(path)
    # __________________________________________________________________________
    headers = {'ns': 'NAMESPACE', 'name': 'NAME', 'host': 'HOST', 'path': 'PATH'}
    for key in ljust:
        ljust[key] = max(ljust[key], len(headers[key]))
    fmt = "| {} | {} | {} | {} |"
    print(fmt.format(*(headers[k].ljust(ljust[k]) for k in ljust)), flush=True)
    print(fmt.format(*('-' * ljust[k] for k in ljust)), flush=True)
    prev_ns = prev_name = prev_host = ""
    for ns in sorted(buffer):
        for name in sorted(buffer[ns]):
            for host in sorted(buffer[ns][name]):
                for path in sorted(buffer[ns][name][host]):
                    print(fmt.format(
                        ns.ljust(ljust['ns']) if ns != prev_ns else ' ' * ljust['ns'],
                        name.ljust(ljust['name']) if (ns, name) != (prev_ns, prev_name) else ' ' * ljust['name'],
                        host.ljust(ljust['host']) if (ns, name, host) != (prev_ns, prev_name, prev_host) else ' ' * ljust['host'],
                        path.ljust(ljust['path'])))
                    prev_ns, prev_name, prev_host = ns, name, host
    # __________________________________________________________________________
    return True


# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
if __name__ == '__main__':
    exit_status = main()
    # __________________________________________________________________________
    sys.exit(not exit_status)
