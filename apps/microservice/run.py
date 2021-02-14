import ray
import time
from statistics import mean
from collections import defaultdict

import apps.net.remote as rmt
import apps.net.ping as ping
from apps.net.util import k8s, s3
from apps.util.stat import stats

_mb = 1
_kb = _mb / 1024
_gb = 1024 * _mb

fm_cmds = {
    "go-fasthttp": "go run server.go -prefork",
    "node-express": "npm install; node server.js",
    "akka-http": "sbt run",
}

fm_proc = {
    "go-fasthttp": "go",
    "node-express": "npm",
    "akka-http": "sbt",
}


# TODO: add file fetch for these frameworks s.t. they actually
#  do some meaningful operations

@ray.remote
def run(run_config: dict, wrks: dict) -> dict:
    """
    Run wrk2 to benchmark go-fasthttp, light-4j, akka.
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
        msvc_servers = wrks[sit][:ns]
        msvc_clients = wrks[cit][ns: ns + nc]
    else:
        msvc_servers = wrks[sit]
        msvc_clients = wrks[cit]

    ex_ip_to_in_ip = k8s.get_worker_external_internal_ip_map()
    nginx_dir = "/etc/nginx"
    config_file = f"{nginx_dir}/perfd-microsvc.conf"
    web_dir = "/var/www/html"
    test_file_name = "test.txt"
    test_file = f"{web_dir}/{test_file_name}"

    fm = run_config["frameworkName"]
    assert fm in fm_cmds, f"run: unsupported framework {fm}"
    fm_dir = f"/home/admin/microservice/{fm}"
    fm_cmd = fm_cmds[fm]

    # Step 0: clear up
    print(rmt.clean_default_apps(msvc_servers + msvc_clients,
                                 extra_app=[fm_proc[run_config["frameworkName"]], "wrk"]))
    print(rmt.cmd_remote(msvc_clients,
                         cmd_=f"sudo chmod 777 {nginx_dir}; "
                              f"sudo rm -rf {test_file} || true >/dev/null &2>1; "
                              f"sudo rm -rf {config_file} || true >/dev/null &2>1",
                         out=True))

    # Step 1: run msvc servers
    file_src = run_config.get("fileSource", "/dev/zero")
    file_size = run_config.get("fileSize", 1)
    rmt.cmd_remote(msvc_servers,
                   cmd_=f"sudo head -c {file_size}KB {file_src} > {test_file}")

    print(rmt.cmd_remote(msvc_servers,
                         cmd_=f"cd {fm_dir}; "
                              f"{fm_cmd} > /dev/null 2>&1 &", out=True))
    print(f"run: {fm_cmd}")
    print("run: waiting for the server to be ready..")
    time.sleep(5)
    print(f"run: msvc servers at public IPs {msvc_servers}")

    # Step 2: start the nginx for client side load balancing
    config_str = _nginx_config.replace("{SERVER_ENTRY}",
                                       "\n".join([f"server {ex_ip_to_in_ip[s]}:8080;"
                                                  for s in msvc_servers]))
    rmt.cmd_remote(msvc_clients, cmd_=f"cat > {config_file} << EOF {config_str}")
    print(rmt.cmd_remote(msvc_clients, cmd_=f"sudo nginx -c {config_file}", out=True))
    time.sleep(1)

    # Step 3: start the wrk2
    _cmd = f"wrk -t{run_config['numClientThread']} " \
           f"-c{run_config['numConn']} " \
           f"-d{run_config['duration']}s " \
           f"-R{run_config['reqRate']} http://localhost:80 --latency"
    print("run:", _cmd, "at", msvc_clients)

    start = time.time()
    raws = list(map(lambda x: x.decode("utf-8"),
                    rmt.cmd_remote(msvc_clients, cmd_=_cmd, out=True)))
    print(f"run: finished in {time.time() - start}s")
    print("run: sample result:\n", raws[0])

    # Step 4: upload logs and post-proc
    s3.dump_and_upload_file(run_config,
                            bucket=run_config["logBucket"],
                            key=s3.path_join(rid, "config"))
    s3.dump_and_upload_file(raws,
                            bucket=run_config["logBucket"],
                            key=s3.path_join(rid, "log"))

    def parse(raw_) -> dict:
        def parse_time(_t):
            if "us" in _t:
                return float(_t.replace("us", "")) / 1000
            elif "ms" in _t:
                return float(_t.replace("ms", ""))
            elif "s" in _t:
                return float(_t.replace("s", "")) * 1000
            else:
                return float(_t.replace("s", ""))

        results = dict()
        state = "start"
        for l in raw_.split("\n"):
            # state transition
            if "HdrHistogram" in l:
                state = "cdf"
            if "#[Mean" in l:
                state = "stat"
            # line parsing
            if state == "cdf":
                if "50.000%" in l:
                    results["lat_50pc"] = parse_time(l.split()[-1])
                elif "75.000%" in l:
                    results["lat_75pc"] = parse_time(l.split()[-1])
                elif "99.000%" in l:
                    results["lat_99pc"] = parse_time(l.split()[-1])
            elif state == "stat":
                if "Requests/sec" in l:
                    results["rps"] = float(l.split()[-1])
                if "Transfer/sec" in l:
                    tput = l.split()[-1]
                    if "MB" in tput:
                        tput = float(tput.replace("MB", ""))
                        results["throughput"] = tput * _mb
                    elif "KB" in tput:
                        tput = float(tput.replace("KB", ""))
                        results["throughput"] = tput * _kb
                    elif "GB" in tput:
                        tput = float(tput.replace("GB", ""))
                        results["throughput"] = tput * _gb
                if "#[Mean" in l:
                    results["lat_mean"] = parse_time(l.split()[2].rstrip(","))
                    results["lat_std"] = parse_time(l.split()[5].rstrip("]"))
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
                "rps",
                "throughput",
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
    lat = mean(ping.bipartite_lats(msvc_servers,
                                   [ex_ip_to_in_ip[i] for i in msvc_clients]))
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
  upstream microservice {
    {SERVER_ENTRY}
  }

  server {
    listen 80;
    location / {
      proxy_pass http://microservice;
    }
  }
}
EOF
"""
