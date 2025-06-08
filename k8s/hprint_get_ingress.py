#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------------------------------------------------
# Print Ingress in human-readable format
# Example:
#   $ kubectl get ingress --all-namespaces -o yaml | ./hprint_get_ingress.py
# ----------------------------------------------------------------------------------------------------------------------
import sys
import traceback

from ruamel.yaml import YAML


def main():
    yaml = YAML()
    yaml.indent(mapping=2, sequence=4, offset=2)
    buffer = dict()
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
    # ------------------------------------------------------------------------------------------------------------------
    for ns in sorted(buffer):
        for idx_name, name in enumerate(sorted(buffer[ns])):
            for idx_host, host in enumerate(sorted(buffer[ns][name])):
                for idx_path, path in enumerate(sorted(buffer[ns][name][host])):
                    print("| {} | {} | {} | {} |".format(
                        ns.ljust(ljust['ns']) if idx_name == 0 and idx_host == 0 and idx_path == 0 else " " * ljust['ns'],
                        name.ljust(ljust['name']) if idx_host == 0 and idx_path == 0 else " " * ljust['name'],
                        host.ljust(ljust['host']) if idx_path == 0 else " " * ljust['host'],
                        path.ljust(ljust['path'])))
    # ------------------------------------------------------------------------------------------------------------------
    return True


# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
if __name__ == '__main__':
    exit_status = main()
    # __________________________________________________________________________
    sys.exit(not exit_status)
