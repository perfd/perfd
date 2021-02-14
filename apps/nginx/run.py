import ray
import time
from datetime import datetime
from statistics import mean
from collections import defaultdict

import apps.net.remote as rmt
import apps.net.ping as ping
import apps.net.iperf as iperf
from apps.net.util import k8s, s3

_mb = 1
_kb = _mb / 1024
_gb = 1024 * _mb


@ray.remote
def run(run_config: dict, wrks: dict) -> dict:
    """
    Run wrk2 to benchmark nginx.
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
        nginx_servers = wrks[sit][:ns]
        nginx_clients = wrks[cit][ns: ns + nc]
    else:
        nginx_servers = wrks[sit]
        nginx_clients = wrks[cit]

    ex_ip_to_in_ip = k8s.get_worker_external_internal_ip_map()
    web_dir = "/var/www/html"
    nginx_dir = "/etc/nginx"
    test_file_name = "test.txt"
    test_file = f"{web_dir}/{test_file_name}"
    config_file = f"{nginx_dir}/perfd-nginx.conf"

    # Step 0: clear up
    print(rmt.clean_default_apps(nginx_servers + nginx_clients,
                                 extra_app=["nginx", "wrk"]))
    print(rmt.cmd_remote(nginx_servers + nginx_clients,
                         cmd_=f"sudo chmod 777 {nginx_dir}; "
                              f"sudo chmod 777 {web_dir}; "
                              f"sudo rm -rf {test_file} || true >/dev/null &2>1; "
                              f"sudo rm -rf {config_file} || true >/dev/null &2>1",
                         out=True))

    # Step 1: prepare files according to given input scale
    file_src = run_config.get("fileSource", "/dev/zero")
    rmt.cmd_remote(nginx_servers,
                   cmd_=f"sudo head -c {run_config['fileSize']}KB {file_src} > {test_file}")

    print(f"run: nginx servers at public IPs {nginx_servers}")

    # Step 2: update the nginx config; start the nginx servers
    config_str = _nginx_config.replace("{numWorkerProc}",
                                       str(run_config["numWorkerProc"]))
    rmt.cmd_remote(nginx_servers, cmd_=f"cat > {config_file} << EOF {config_str}")

    if len(nginx_servers) > 1:
        # TODO: multiple servers (may not needed as we just need numWorkerProc scaling)
        raise Exception("run: unimplemented multiple server")
    else:
        svc_ip = ex_ip_to_in_ip[nginx_servers[0]]
        rmt.cmd_remote(nginx_servers, cmd_=f"sudo nginx -c {config_file}")

    # Step 3: start the wrk2
    _cmd = f"wrk -t{run_config['numClientThread']} " \
           f"-c{run_config['numConn']} " \
           f"-d{run_config['duration']}s " \
           f"-R{run_config['reqRate']} http://{svc_ip}/{test_file_name} --latency"

    print("run:", _cmd)
    start = time.time()
    raws = list(map(lambda x: x.decode("utf-8"),
                    rmt.cmd_remote(nginx_clients, cmd_=_cmd, out=True)))
    print(f"run: finished in {time.time() - start}s")
    print("run results, sample:\n", raws[0])

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

    # pair wise throughput info
    if run_config.get("iperfProfile", False):
        iperf_server = nginx_servers[0]
        iperf_client = nginx_clients[0]
        rmt.cmd_remote([iperf_server, iperf_client],
                       cmd_=f"sudo apt install iperf3 -y")

        iperf.start_server(iperf_server)
        out = iperf.start_client(iperf_client, iperf_server, out=True)
        tput = out["end"]["sum_received"]["bits_per_second"] / (1024 * 1024)
        r.update({
            "avg_client_server_tput": tput,
        })

    # pair wise latency info
    lat = mean(ping.bipartite_lats(nginx_servers, [ex_ip_to_in_ip[i] for i in nginx_clients]))

    # lat and timestamp
    r.update({
        "avg_client_server_lat": lat,
        "machinetime": time.time(),
        "datetime": datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
    })

    r.update(run_config)
    return r


_nginx_config = """
user www-data;
worker_processes {numWorkerProc};
pid /run/nginx.pid;
include /etc/nginx/modules-enabled/*.conf;

events {
        worker_connections 768;
        # multi_accept on;
}

http {
        ##
        # Basic Settings
        ##
    
        sendfile on;
        tcp_nopush on;
        tcp_nodelay on;
        keepalive_timeout 65;
        types_hash_max_size 2048;
        # server_tokens off;
    
        # server_names_hash_bucket_size 64;
        # server_name_in_redirect off;
    
        include /etc/nginx/mime.types;
        default_type application/octet-stream;
    
        ##
        # SSL Settings
        ##
    
        ssl_protocols TLSv1 TLSv1.1 TLSv1.2; # Dropping SSLv3, ref: POODLE
        ssl_prefer_server_ciphers on;
    
        ##
        # Logging Settings
        ##
    
        access_log /var/log/nginx/access.log;
        error_log /var/log/nginx/error.log;
    
        ##
        # Gzip Settings
        ##
    
        gzip on;
        gzip_disable "msie6";
    
        # gzip_vary on;
        # gzip_proxied any;
        # gzip_comp_level 6;
        # gzip_buffers 16 8k;
        # gzip_http_version 1.1;
        # gzip_types text/plain text/css application/json application/javascript text/xml application/xml application/xml+rss text/javascript;
    
        ##
        # Virtual Host Configs
        ##
    
        include /etc/nginx/conf.d/*.conf;
        include /etc/nginx/sites-enabled/*;
}

EOF
"""
