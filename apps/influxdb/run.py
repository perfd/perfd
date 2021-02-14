import ray
import time
from statistics import mean
from collections import defaultdict

import apps.net.remote as rmt
import apps.net.ping as ping
from apps.net.util import k8s, s3
from apps.util.stat import stats


@ray.remote
def run(run_config: dict, wrks) -> dict:
    """
    Run inch to benchmark influxdb
    """
    # get workers
    rid = run_config["run_id"]
    print("run: assume the remote VM image contains all deps; "
          "nothing to install;")

    # get servers and clients
    sit = run_config["serverInstanceType"]
    cit = run_config["clientInstanceType"]
    if sit == cit:
        ns = run_config["numServerInstance"]
        nc = run_config["numClientInstance"]
        influx_servers = wrks[sit][:ns]
        influx_clients = wrks[cit][ns: ns + nc]
    else:
        influx_servers = wrks[sit]
        influx_clients = wrks[cit]

    nginx_dir = "/etc/nginx"
    config_file = f"{nginx_dir}/perfd-influxdb.conf"

    ex_ip_to_in_ip = k8s.get_worker_external_internal_ip_map()

    # Step 0: clear up
    print(rmt.clean_default_apps(influx_servers + influx_clients,
                                 extra_app=["influxd", "inch"],
                                 docker_cont=["influxd"]))
    print(rmt.cmd_remote(influx_clients,
                         cmd_=f"sudo chmod 777 {nginx_dir}; "
                              f"sudo rm -rf {config_file} || true >/dev/null &2>1",
                         out=True))

    # Step 1: run influxd servers
    """docker run -p 8086:8086 \
      -v $PWD:/var/lib/influxdb \
      influxdb"""

    # demux run commands based on runner type
    runner_type = run_config.get("runner", "bare")
    if runner_type == "bare":
        cmd_ = f"sudo influxd > /dev/null 2>&1 &"
    elif runner_type == "docker":
        tag = run_config.get("tag", "1.7.10")
        cmd_ = f"sudo docker run --name influxd -d -p 8086:8086 " \
               f"-v $PWD:/var/lib/influxdb influxdb:{tag} > /dev/null 2>&1 &"
        print(f"run: use docker image influxdb:{tag}")
    else:
        raise Exception(f"run: unknown runner type {runner_type}")

    # start servers
    print(f"run: using {runner_type} runner type with command {cmd_}")
    print(rmt.cmd_remote(influx_servers, cmd_=cmd_, out=True))
    print(f"run: influxd servers at public IPs {influx_servers}")
    print("run: waiting for the server to be ready..")
    time.sleep(5)

    # Step 2: start the nginx for client side load balancing
    config_str = _nginx_config.replace("{SERVER_ENTRY}",
                                       "\n".join([f"server {ex_ip_to_in_ip[s]}:8086;"
                                                  for s in influx_servers]))

    rmt.cmd_remote(influx_clients, cmd_=f"cat > {config_file} << EOF {config_str}")
    print(rmt.cmd_remote(influx_clients, cmd_=f"sudo nginx -c {config_file}", out=True))

    # Step 3: start the inch
    _cmd = f"for r in {{1..{run_config['numReq']}}}; do inch " \
           f"-host http://localhost:80 " \
           f"-p {run_config['numPointPerSeries']} & \n done; wait"

    print("run:", _cmd, f"at public IPs {influx_clients}")

    start = time.time()
    raws = list(map(lambda x: x.decode("utf-8"),
                    rmt.cmd_remote(influx_clients, cmd_=_cmd, out=True)))
    # print("debug:", raws)
    print(f"run: finished in {time.time() - start}s")
    print("run results, sample:\n", raws[0])

    # print("debug:", raws)
    # Step 4: upload logs and post-proc
    s3.dump_and_upload_file(run_config,
                            bucket=run_config["logBucket"],
                            key=s3.path_join(rid, "config"))
    s3.dump_and_upload_file(raws,
                            bucket=run_config["logBucket"],
                            key=s3.path_join(rid, "log"))

    def parse(raw_) -> dict:
        results = dict()
        lats = list()
        for l in raw_.split("\n"):
            if "Total time:" in l:
                lats.append(float(l.split()[2]))

        for k, v in stats(lats).items():
            results[f"query_latency_{k}"] = v
        return results

    def agg(rs: list) -> dict:
        ag = defaultdict(list)
        for _r in rs:
            for k, v in _r.items():
                # all values default to float
                ag[k].append(float(v))
        # sum
        for k, v in ag.items():
            # sum
            if k in {
            }:
                ag[k] = sum(ag[k])
            # default to avg
            else:
                ag[k] = mean(ag[k])
        return ag

    r = dict()
    r.update(agg([parse(r) for r in raws]))
    print("run: results", r)
    r.update(run_config)

    # pair wise latency info
    lat = mean(ping.bipartite_lats(influx_servers,
                                   [ex_ip_to_in_ip[i] for i in influx_clients]))
    r.update({
        "avg_client_server_lat": lat,
    })

    return r


_nginx_config = """
events {
        worker_connections 768;
        # multi_accept on;
}

http {
  upstream influxdb {
    {SERVER_ENTRY}
  }

  server {
    listen 80;
    location / {
      proxy_pass http://influxdb;
    }
  }
}
EOF
"""
