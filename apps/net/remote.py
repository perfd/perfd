import ray
import time

from .util import cmd, k8s

_ssh_str = "ssh -o \"StrictHostKeyChecking no\" -o \"LogLevel=ERROR\" admin@{host_ip} \"{cmd}\""
_skip_interfaces = {"docker0", "lo"}


# remote cmd utilities
def worker_ips() -> list:
    return list(k8s.get_worker_external_ip_map().values())


def cmd_remote(host_ips: list, cmd_: str, out=False, async_=False, parallel=True):
    if parallel:
        futures = [_cmd.remote(_ssh_str.format(host_ip=hi, cmd=cmd_), out, async_)
                   for hi in host_ips]
        return ray.get(futures)
    else:
        return [cmd(_ssh_str.format(host_ip=hi, cmd=cmd_),
                    out=out,
                    async_=async_)
                for hi in host_ips]


@ray.remote
def _cmd(cmd_, out, async_):
    return cmd(cmd_, out=out, async_=async_)


def scp(host_ips: list, src_path, dest_path, user="admin"):
    for ip in host_ips:
        cmd_ = "scp %s@%s:%s %s" % (user, ip, src_path, dest_path);
        print(cmd_)
        cmd(cmd_)


def get_external_interfaces(host_ip):
    results = cmd_remote([host_ip], "sudo ifconfig | grep \".*flags\" | cut -d\":\" -f1")[0].decode(
        "ascii").rstrip()
    results = results.split("\n")

    return {i for i in results if i not in _skip_interfaces}


# tc utilities
def install_utils(host_ips: list):
    cmd = "sudo apt install iproute iperf3 iperf tcpdump -y &"
    cmd_remote(host_ips, cmd, async_=True)
    time.sleep(5)


def clean_default_apps(host_ips: list, extra_app=None, docker_cont=None):
    cmd = "sudo pkill memcached; " \
          "sudo pkill influxd; " \
          "sudo pkill nginx; "

    if extra_app is not None:
        for a in extra_app:
            cmd += f"sudo pkill {a}; "

    if docker_cont is not None:
        for d in docker_cont:
            cmd += f"sudo docker stop {d}; sudo docker rm {d}; "
    return cmd_remote(host_ips, cmd, out=True)
