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
    cw = {  # column width per field, used for .ljust() padding
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
        if len(ns) > cw.get('ns', 0):
            cw['ns'] = len(ns)
        #
        name = item['metadata']['name']
        buffer[ns].setdefault(name, dict())
        if len(name) > cw.get('name', 0):
            cw['name'] = len(name)
        #
        for rule in item['spec']['rules']:
            host = rule['host']
            buffer[ns][name].setdefault(host, list())
            if len(host) > cw.get('host', 0):
                cw['host'] = len(host)
            #
            for path in rule['http']['paths']:
                path = path.get('path', '')
                buffer[ns][name][host].append(path)
                if len(path) > cw.get('path', 0):
                    cw['path'] = len(path)
    # __________________________________________________________________________
    headers = {'ns': 'NAMESPACE', 'name': 'NAME', 'host': 'HOST', 'path': 'PATH'}
    for key in cw:
        cw[key] = max(cw[key], len(headers[key]))
    fmt = "| {} | {} | {} | {} |"
    print(fmt.format(*(headers[k].ljust(cw[k]) for k in cw)), flush=True)
    print(fmt.format(*('-' * cw[k] for k in cw)), flush=True)
    prev_ns = prev_name = prev_host = ""
    for ns in sorted(buffer):
        for name in sorted(buffer[ns]):
            for host in sorted(buffer[ns][name]):
                for path in sorted(buffer[ns][name][host]):
                    print(fmt.format(
                        ns.ljust(cw['ns']) if ns != prev_ns else ' ' * cw['ns'],
                        name.ljust(cw['name']) if (ns, name) != (prev_ns, prev_name) else ' ' * cw['name'],
                        host.ljust(cw['host']) if (ns, name, host) != (prev_ns, prev_name, prev_host) else ' ' * cw['host'],
                        path.ljust(cw['path'])))
                    prev_ns, prev_name, prev_host = ns, name, host
    # __________________________________________________________________________
    return True


# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
if __name__ == '__main__':
    exit_status = main()
    # __________________________________________________________________________
    sys.exit(not exit_status)
