import ray
import time
from statistics import mean
from collections import defaultdict

import apps.net.remote as rmt
import apps.net.ping as ping
from apps.net.util import k8s, s3

from thirdparty.microps.oracle.feature.cloud.gen_aws_ec2 import aws_resource_map


@ray.remote
def run(run_config: dict, wrks: dict) -> dict:
    """
    Run memcached benchmark with fixed configurations.

    Returns a list consisting of results from multiple runs, where
    each result is a map of k-v pairs.
    """

    def validate():
        for k in {"keySize", "valueSize",
                  "serverThread", "clientThread",
                  "runTime", "waitTime", "warmupTime"}:
            assert k in run_config, f"run: missing config entry '{k}', abort"

    validate()

    rid = run_config["run_id"]

    # get servers and clients
    sit = run_config["serverInstanceType"]
    cit = run_config["clientInstanceType"]
    if sit == cit:
        ns = run_config["numServerInstance"]
        nc = run_config["numClientInstance"]
        mcd_servers = wrks[sit][:ns]
        mut_clients = wrks[cit][ns: ns + nc]
    else:
        mcd_servers = wrks[sit]
        mut_clients = wrks[cit]

    # install deps and clean up
    print("run: assume the remote VM image contains all deps; "
          "nothing to install;")
    print(rmt.clean_default_apps(mcd_servers + mut_clients,
                                 extra_app=["memcached", "mutilate"],
                                 docker_cont=["memcached"]))

    ex_ip_to_in_ip = k8s.get_worker_external_internal_ip_map()

    # Step 1: start the memcached servers
    # get memcached server IPs (internal VPC IP); we are
    # not using a load balancer here, mutilate does client-side
    # load balancing already
    port = 11211
    server_ex_ips = mcd_servers
    server_in_ips = [ex_ip_to_in_ip[e] for e in server_ex_ips]
    client_ex_ips = mut_clients
    client_in_ips = [ex_ip_to_in_ip[i] for i in client_ex_ips]

    num_server_thread = run_config.get("serverThread", -1)
    if num_server_thread < 0:
        num_server_thread = aws_resource_map[run_config["serverInstanceType"]]["vCPUs"]
        run_config["serverThread"] = num_server_thread

    # demux server runner type, default run on bare metal
    runner_type = run_config.get("runner", "bare")
    if runner_type == "bare":
        cmd_ = f"memcached -t {num_server_thread} -c 32768 > /dev/null 2>&1 & "
        rmt.cmd_remote(mcd_servers, cmd_=cmd_)
    elif runner_type == "docker":
        # default tag: 1.4.33
        tag = run_config.get("tag", "1.4.33")

        # run the container
        cmd_ = f"sudo docker run --name memcached -d -p {port}:{port} memcached:{tag} " \
               f"memcached -t {num_server_thread} -c 32768 > /dev/null 2>&1 & "
        rmt.cmd_remote(mcd_servers, cmd_=cmd_)

        # wait a bit for the container to be ready
        time.sleep(5)
        print(f"run: docker image memcached:{tag}")
    else:
        raise Exception(f"run: unknown runner type {runner_type}")
    print(f"run: using {runner_type} runner type")
    print(f"run: memcached servers at internal IPs {server_in_ips}, public IPs {server_ex_ips} with {cmd_}")

    # Step 2: start the mutilate agents
    master = mut_clients[0]
    agents = mut_clients[1:]

    if len(agents) >= 1:
        _cmd_agent = f"mutilate -T {run_config['clientThread']} " \
                     f"-K {run_config['keySize']} " \
                     f"-V {run_config['valueSize']} " \
                     f"-c 4 " \
                     f"-A > /dev/null 2>&1 & "
        print("run: agents", agents, _cmd_agent)
        rmt.cmd_remote(agents, cmd_=_cmd_agent)

    # Step 3: start the mutilate master runner
    # TODO: add input distribution knob
    def make_master_cmd():
        server_str = " ".join([f"-s {si}:{port}" for si in server_in_ips])
        agent_str = " ".join([f"-a {ex_ip_to_in_ip[ax]}" for ax in agents])
        option_str = f"-T {run_config['clientThread']} " \
                     f"-K {run_config['keySize']} " \
                     f"-V {run_config['valueSize']} " \
                     f"-t {run_config['runTime']} " \
                     f"-w {run_config['warmupTime']} " \
                     f"-c 1 " \
                     f"-W {run_config['waitTime']} --noload"
        return f"mutilate {server_str} --loadonly", \
               f"mutilate {server_str} {agent_str} {option_str}"

    _cmd_load, _cmd_run = make_master_cmd()

    print("run: master", master, _cmd_run)
    start = time.time()
    rmt.cmd_remote([master], cmd_=_cmd_load)

    raw = rmt.cmd_remote([master], cmd_=_cmd_run, out=True)[0].decode("utf-8")
    print(f"run: finished in {time.time() - start}s")
    print("run results, sample:\n", raw)

    # Step 4: upload logs
    s3.dump_and_upload_file(run_config,
                            bucket=run_config["logBucket"],
                            key=s3.path_join(rid, "config"))
    s3.dump_and_upload_file(raw,
                            bucket=run_config["logBucket"],
                            key=s3.path_join(rid, "log"))

    # Step 5: parse and aggregate results
    def parse(_raw) -> dict:
        _raw = _raw.split("\n")
        results = dict()
        for l in _raw:
            vs = l.split()
            if len(vs) < 1:
                continue
            v_type, v = vs[0], None
            if v_type == "read":
                v = {"avg_lat_read": vs[1],
                     "std_lat_read": vs[2],
                     "min_lat_read": vs[3],
                     "99th_lat_read": vs[8],
                     }
            elif v_type.startswith("Total"):
                v = {"qps": vs[3]}
            elif v_type.startswith("RX"):
                v = {"rx_goodput": vs[-2]}
            elif v_type.startswith("TX"):
                v = {"tx_goodput": vs[-2]}
            if v is not None:
                results.update(v)
        return results

    r = dict()
    r.update(parse(raw))
    print("run: results", r)

    r.update(run_config)

    # pair wise latency info
    lat = mean(ping.bipartite_lats(mcd_servers, client_in_ips))
    r.update({
        "avg_client_server_lat": lat,
    })

    # debugging info
    r.update({
        "debug_num_server": len(mcd_servers),
        "debug_num_client": len(mut_clients),
        "debug_num_agent": len(agents),
        "debug_client_ex_IPs": mut_clients,
        "debug_server_ex_IPs": mcd_servers,
        "debug_client_in_IPs": client_in_ips,
        "debug_server_in_IPs": server_in_ips,
    })
    return r
