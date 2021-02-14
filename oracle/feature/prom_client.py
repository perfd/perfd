import csv
import requests
import sys

import oracle.utils as oc_utils

_default_server_url = "http://localhost:9090"


class PromClient:
    """Usage: call get_container_metrics_names(), use the metrics to call
    make_prom_batch_query_on_pod() and obtain the query, then use query() to
    launch the query."""

    def __init__(self):
        pass

    @staticmethod
    def get_container_metrics_names(prom_url=_default_server_url):
        response = requests.get('{0}/api/v1/label/__name__/values'.format(prom_url))
        names = response.json()['data']
        return [n for n in names if n.startswith("container")]

    @staticmethod
    def make_prom_batch_query_on_pod(metrics, pod_name, time_range="1d"):
        return PromClient.make_prom_batch_query(metrics, "{{__name__ =~ \"{}\", pod_name =~ \""
                                                + pod_name + "\"}}[" + time_range + "]")

    @staticmethod
    def make_prom_batch_query(metrics, base_str=None, time_range="1d"):
        base_str = "{{__name__ =~ \"{}\"}}[" + time_range + "]" if base_str is None else base_str
        if type(metrics) is list:
            metrics = set(metrics)
            metrics_str = "|".join(metrics)
        else:
            metrics_str = metrics
        query = base_str.format(metrics_str)

        return query

    @staticmethod
    def query(q: str, prom_url=_default_server_url) -> dict:
        response = requests.get('{0}/api/v1/query'.format(prom_url),
                                params={'query': q})
        results = response.json()['data']['result']
        return results

    @staticmethod
    def backup(prom_url=_default_server_url):
        """TODO"""
        pass

    @staticmethod
    def export_csv():
        """A simple program to print the result of a Prometheus query as CSV."""

        if len(sys.argv) != 3:
            print('Usage: {0} http://prometheus:9090 a_query'.format(sys.argv[0]))
            sys.exit(1)

        response = requests.get('{0}/api/v1/query'.format(sys.argv[1]),
                                params={'query': sys.argv[2]})
        results = response.json()['data']['result']

        # Build a list of all label names used.
        label_names = set()
        for result in results:
            label_names.update(result['metric'].keys())

        # Canonicalize
        label_names.discard('__name__')
        label_names = sorted(label_names)

        writer = csv.writer(sys.stdout)
        # Write the header,
        writer.writerow(['name', 'timestamp', 'value'] + label_names)

        # Write the samples.
        for result in results:
            l = [result['metric'].get('__name__', '')] + result['value']
            for label in label_names:
                l.append(result['metric'].get(label, ''))
            writer.writerow(l)

    @staticmethod
    def take_snapshot():
        """Take a snapshot and return the snapshot id. Default path is /prometheus/snapshots"""
        r = oc_utils.cmd_out("curl -XPOST http://localhost:9090/api/v1/admin/tsdb/snapshot")
        return str(r).split("\"name\":\"")[1].split("\"")[0]

    @staticmethod
    def get_snapshot(id_=None):
        # TODO:
        if id_ is None:
            id_ = PromClient.take_snapshot()
        pass


if __name__ == '__main__':
    pc = PromClient()
    # pp.pprint(pc.get_container_metrics_names())
    pc.take_snapshot()
    q = pc.make_prom_batch_query_on_pod("node.*", "sparklr-.*")
    # results = pc.query(q)
    # pp.pprint(results)
