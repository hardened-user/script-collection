#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------------------------------------------------
import argparse
import sys
import traceback

from ruamel.yaml import YAML

MEM_UNITS = (  # longest suffix first, so 'Ei' is matched before 'E'
    ('Ei', 2 ** 60), ('Pi', 2 ** 50), ('Ti', 2 ** 40), ('Gi', 2 ** 30), ('Mi', 2 ** 20), ('Ki', 2 ** 10),
    ('E', 10 ** 18), ('P', 10 ** 15), ('T', 10 ** 12), ('G', 10 ** 9), ('M', 10 ** 6), ('K', 10 ** 3)
)
MEM_BINARY_UNITS = MEM_UNITS[:6]


def main():
    # __________________________________________________________________________
    # command-line options, arguments
    try:
        parser = argparse.ArgumentParser(
            description='Print Kubernetes Pod resource requests/limits in human-readable table format.',
            epilog='example:\n\tkubectl get pods --all-namespaces -o yaml | %(prog)s',
            formatter_class=argparse.RawDescriptionHelpFormatter
        )
        parser.add_argument('-n', '--namespace', action='store', type=str, default=None,
                            metavar='<NS>', help="filter output by namespace")
        parser.add_argument('-t', '--total', action='store_true',
                            help="print only the per-namespace summary table")
        sort_group = parser.add_mutually_exclusive_group()
        sort_group.add_argument('-c', '--cpu', action='store_true',
                                help="sort output by pod's total cpu request, ascending")
        sort_group.add_argument('-m', '--memory', action='store_true',
                                help="sort output by pod's total memory request, ascending")
        args = parser.parse_args()  # <class 'argparse.Namespace'>
    except SystemExit:
        return False
    # __________________________________________________________________________
    yaml = YAML(typ='safe')  # read-only, no need for the round-trip loader
    buffer: dict[str, dict[str, dict[str, dict[str, str]]]] = dict()
    cw = {  # column width per field, used for .ljust() padding
        'ns': 0,
        'pod': 0,
        'container': 0,
        'cpu_req': 0,
        'mem_req': 0,
        'cpu_limit': 0,
        'mem_limit': 0
    }
    total = {'cpu_req': 0.0, 'mem_req': 0.0, 'cpu_limit': 0.0, 'mem_limit': 0.0}
    ns_order: list[str] = list()
    ns_total: dict[str, dict[str, float]] = dict()
    pod_order: dict[str, list[str]] = dict()
    pod_total: dict[tuple[str, str], dict[str, float]] = dict()
    try:
        pods_yaml = yaml.load(stream=sys.stdin)
        items = pods_yaml['items']
    except Exception as err:
        print(f"[!!] Exception: {type(err)}\n{''.join(traceback.format_exc(limit=1))}", flush=True)
        return False
    for item in items:
        if item.get('status', {}).get('phase') in ('Succeeded', 'Failed'):
            continue
        ns = item['metadata']['namespace']
        if args.namespace and ns != args.namespace:
            continue
        pod = item['metadata']['name']
        containers = item['spec']['containers']
        if ns not in buffer:
            buffer[ns] = dict()
            ns_order.append(ns)
            ns_total[ns] = {'cpu_req': 0.0, 'mem_req': 0.0, 'cpu_limit': 0.0, 'mem_limit': 0.0}
            pod_order[ns] = list()
        if len(ns) > cw.get('ns', 0):
            cw['ns'] = len(ns)
        #
        buffer[ns].setdefault(pod, dict())
        if len(pod) > cw.get('pod', 0):
            cw['pod'] = len(pod)
        #
        pod_order[ns].append(pod)
        pod_total[(ns, pod)] = {'cpu': 0.0, 'mem': 0.0}
        for container in containers:
            name = container['name']
            if len(name) > cw.get('container', 0):
                cw['container'] = len(name)
            #
            resources = container.get('resources') or dict()
            requests = resources.get('requests') or dict()
            limits = resources.get('limits') or dict()
            row = {
                'cpu_req': str(requests['cpu']) if 'cpu' in requests else '-',
                'mem_req': str(requests['memory']) if 'memory' in requests else '-',
                'cpu_limit': str(limits['cpu']) if 'cpu' in limits else '-',
                'mem_limit': str(limits['memory']) if 'memory' in limits else '-'
            }
            buffer[ns][pod][name] = row
            for key in ('cpu_req', 'mem_req', 'cpu_limit', 'mem_limit'):
                if len(row[key]) > cw.get(key, 0):
                    cw[key] = len(row[key])
            #
            if 'cpu' in requests:
                cpu_req = parse_cpu(requests['cpu'])
                total['cpu_req'] += cpu_req
                ns_total[ns]['cpu_req'] += cpu_req
                pod_total[(ns, pod)]['cpu'] += cpu_req
            if 'memory' in requests:
                mem_req = parse_mem(requests['memory'])
                total['mem_req'] += mem_req
                ns_total[ns]['mem_req'] += mem_req
                pod_total[(ns, pod)]['mem'] += mem_req
            if 'cpu' in limits:
                cpu_limit = parse_cpu(limits['cpu'])
                total['cpu_limit'] += cpu_limit
                ns_total[ns]['cpu_limit'] += cpu_limit
            if 'memory' in limits:
                mem_limit = parse_mem(limits['memory'])
                total['mem_limit'] += mem_limit
                ns_total[ns]['mem_limit'] += mem_limit
    # __________________________________________________________________________
    headers = {
        'ns': 'NAMESPACE', 'pod': 'POD', 'container': 'CONTAINER',
        'cpu_req': 'CPU REQ', 'mem_req': 'MEM REQ', 'cpu_limit': 'CPU LIMIT', 'mem_limit': 'MEM LIMIT'
    }
    total_str = {
        'cpu_req': fmt_cpu(total['cpu_req']), 'mem_req': fmt_mem(total['mem_req']),
        'cpu_limit': fmt_cpu(total['cpu_limit']), 'mem_limit': fmt_mem(total['mem_limit'])
    }
    ns_total_str = {
        ns: {
            'cpu_req': fmt_cpu(ns_total[ns]['cpu_req']), 'mem_req': fmt_mem(ns_total[ns]['mem_req']),
            'cpu_limit': fmt_cpu(ns_total[ns]['cpu_limit']), 'mem_limit': fmt_mem(ns_total[ns]['mem_limit'])
        } for ns in ns_order
    }
    for key in cw:
        cw[key] = max(cw[key], len(headers[key]))
    for key in total_str:
        cw[key] = max(cw[key], len(total_str[key]))
    for row in ns_total_str.values():
        for key in row:
            cw[key] = max(cw[key], len(row[key]))
    # __________________________________________________________________________
    if args.total:
        cols = ('ns', 'cpu_req', 'mem_req', 'cpu_limit', 'mem_limit')
        fmt = "| {} | {} | {} | {} | {} |"
        print(fmt.format(*(headers[k].ljust(cw[k]) for k in cols)), flush=True)
        print(fmt.format(*('-' * cw[k] for k in cols)), flush=True)
        ns_list = ns_order
        if args.cpu:
            ns_list = sorted(ns_list, key=lambda n: ns_total[n]['cpu_req'])
        elif args.memory:
            ns_list = sorted(ns_list, key=lambda n: ns_total[n]['mem_req'])
        for ns in ns_list:
            print(fmt.format(
                ns.ljust(cw['ns']),
                ns_total_str[ns]['cpu_req'].ljust(cw['cpu_req']),
                ns_total_str[ns]['mem_req'].ljust(cw['mem_req']),
                ns_total_str[ns]['cpu_limit'].ljust(cw['cpu_limit']),
                ns_total_str[ns]['mem_limit'].ljust(cw['mem_limit'])), flush=True)
        print(fmt.format(*('-' * cw[k] for k in cols)), flush=True)
        print(fmt.format(
            'TOTAL'.ljust(cw['ns']),
            total_str['cpu_req'].ljust(cw['cpu_req']),
            total_str['mem_req'].ljust(cw['mem_req']),
            total_str['cpu_limit'].ljust(cw['cpu_limit']),
            total_str['mem_limit'].ljust(cw['mem_limit'])), flush=True)
    else:
        fmt = "| {} | {} | {} | {} | {} | {} | {} |"
        print(fmt.format(*(headers[k].ljust(cw[k]) for k in cw)), flush=True)
        print(fmt.format(*('-' * cw[k] for k in cw)), flush=True)
        prev_ns = prev_pod = ""
        for ns in ns_order:
            pods = pod_order[ns]
            if args.cpu:
                pods = sorted(pods, key=lambda p: pod_total[(ns, p)]['cpu'])
            elif args.memory:
                pods = sorted(pods, key=lambda p: pod_total[(ns, p)]['mem'])
            for pod in pods:
                for container in sorted(buffer[ns][pod]):
                    row = buffer[ns][pod][container]
                    print(fmt.format(
                        ns.ljust(cw['ns']) if ns != prev_ns else ' ' * cw['ns'],
                        pod.ljust(cw['pod']) if (ns, pod) != (prev_ns, prev_pod) else ' ' * cw['pod'],
                        container.ljust(cw['container']),
                        row['cpu_req'].ljust(cw['cpu_req']),
                        row['mem_req'].ljust(cw['mem_req']),
                        row['cpu_limit'].ljust(cw['cpu_limit']),
                        row['mem_limit'].ljust(cw['mem_limit'])))
                    prev_ns, prev_pod = ns, pod
        print(fmt.format(*('-' * cw[k] for k in cw)), flush=True)
        print(fmt.format(
            'TOTAL'.ljust(cw['ns']), ' ' * cw['pod'], ' ' * cw['container'],
            total_str['cpu_req'].ljust(cw['cpu_req']),
            total_str['mem_req'].ljust(cw['mem_req']),
            total_str['cpu_limit'].ljust(cw['cpu_limit']),
            total_str['mem_limit'].ljust(cw['mem_limit'])), flush=True)
    # __________________________________________________________________________
    return True


# ======================================================================================================================
# Functions
# ======================================================================================================================
def parse_cpu(value) -> float:
    value = str(value)
    if value.endswith('m'):
        return float(value[:-1])
    return float(value) * 1000


def parse_mem(value) -> float:
    value = str(value)
    for suffix, size in MEM_UNITS:
        if value.endswith(suffix):
            return float(value[:-len(suffix)]) * size
    return float(value)


def fmt_cpu(millicores: float) -> str:
    millicores = round(millicores)
    if millicores == 0:
        return '-'
    if millicores >= 1000:
        return f"{millicores / 1000:g}"
    return f"{millicores}m"


def fmt_mem(num_bytes: float) -> str:
    num_bytes = round(num_bytes)
    if num_bytes == 0:
        return '-'
    for suffix, size in MEM_BINARY_UNITS:
        if num_bytes >= size:
            return f"{num_bytes / size:g}{suffix}"
    return f"{num_bytes}"


# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
if __name__ == '__main__':
    exit_status = main()
    # __________________________________________________________________________
    sys.exit(not exit_status)
