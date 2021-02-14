import time
import json

from .util import k8s
from . import remote as rt

"""Simple iperf examples.

TODO: check if iperf multi-client, single-server
TODO: make iperf container and pod
"""


def start_client(ip, server_ip, iperf_str="iperf3", *,
                 interval=1, arg_str="", out=False):
    # preparation
    server_ip = k8s.get_worker_external_internal_ip_map()[server_ip]
    rt.cmd_remote(host_ips=[ip],
                  cmd_="pkill %s" % iperf_str,
                  out=False)

    # start client
    cmd_ = iperf_str + " " + "-J -i %s -c %s %s" % (interval, server_ip, arg_str)
    print("tc.iperf: starting client: %s with config: %s.." % (ip, cmd_))
    out = json.loads(rt.cmd_remote(host_ips=[ip],
                                   cmd_=cmd_,
                                   out=out)[0].decode())
    return out


def start_server(ip, iperf_str="iperf3", *,
                 arg_str="", ready_wait=2):
    # preparation
    rt.cmd_remote(host_ips=[ip],
                  cmd_="pkill %s" % iperf_str,
                  out=False)

    # start server
    cmd_ = iperf_str + " " + "-s %s" % arg_str
    print("tc.iperf: starting server: %s with config: %s.." % (ip, cmd_))
    rt.cmd_remote(host_ips=[ip],
                  cmd_=cmd_,
                  async_=True)

    time.sleep(ready_wait)
