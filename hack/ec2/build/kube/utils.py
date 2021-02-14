import subprocess
import sys
import time
import argparse
import os
import yaml
import uuid
import datetime

from typing import Dict

mb = 1.024 * pow(10, 6)
gb = mb * pow(10, 3)
dir_path = os.path.dirname(os.path.realpath(__file__))
_script_dir = dir_path + "/scripts/"


def get_worker_instance_types_static() -> Dict[str, int]:
    """Returns worker instance types of kops instance group and their number defined in the static manifest."""
    with open(dir_path + "/scripts/foo.k8s.local.yaml", "r") as f:
        cluster_configs = yaml.load_all(f)
        # assume the max_size == min_size
        return {config["spec"]["machineType"]: config["spec"]["minSize"]
                for config in cluster_configs
                if config["kind"] == "InstanceGroup" and config["spec"]["role"] == "Node"}


def yaml_load(file_):
    with open(file_, "r") as f:
        return yaml.load(f)


def yaml_loads(str_):
    return yaml.load(str_)


def yaml_load_all(file_):
    with open(file_, "r") as f:
        return list(yaml.safe_load_all(f))


def yaml_dump_all_str(to_dump: list) -> str:
    for td in to_dump:
        td["metadata"]["creationTimestamp"] = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
    return yaml.safe_dump_all(to_dump, default_flow_style=False)


def yaml_dump_file(to_dump: dict, file_):
    with open(file_, "w") as f:
        f.write(yaml_dump_str(to_dump))


def yaml_dump_all_file(to_dump: list, file_):
    with open(file_, "w") as f:
        f.write(yaml_dump_all_str(to_dump))


def yaml_dump_str(to_dump: dict) -> str:
    """Return k8s compatible yaml."""
    to_dump["metadata"]["creationTimestamp"] = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
    return yaml.safe_dump(to_dump, default_flow_style=False)


def get_worker_instance_types_all():
    return {
        "m4.large": None,
        "r4.large": None,
        "c4.large": None,
        "m4.xlarge": None,
        "r4.xlarge": None,
        "c4.xlarge": None,
        "m4.2xlarge": None,
        "r4.2xlarge": None,
        "c4.2xlarge": None,
        "m4.4xlarge": None,
        "r4.4xlarge": None,
        "c4.4xlarge": None,
    }


def uuid_str():
    return str(uuid.uuid4())


def change_dir(f):
    # TODO: fix these legacy directory dependencies
    def execute(*args, **kwargs):
        _cur_dir = os.getcwd()
        os.chdir(_script_dir)
        f(*args, **kwargs)
        os.chdir(_cur_dir)

    return execute


def ts_gen():
    return time.strftime("%Y%m%d%H%M%S", time.localtime())


def time_func(f):
    def f_timed(*args, **kwargs):
        start = time.time()
        result = f(*args, **kwargs)
        end = time.time()
        elapsed_time = end - start
        print("--> {function} took {time} seconds.".format(function=f.__name__, time=elapsed_time))
        return result

    return f_timed


def timed(method):
    def timeit(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('%r  %2.2f s' % (method.__name__, (te - ts) * 1))
        return result

    return timeit


def cmd(cmd, quiet=False):
    """Executes a subprocess running a shell command and returns the output."""
    if quiet:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=True,
            executable='/bin/bash')
    else:
        proc = subprocess.Popen(cmd, shell=True, executable='/bin/bash')

    out, _ = proc.communicate()

    if proc.returncode:
        if quiet:
            print('Log:\n', out, file=sys.stderr)
        print('Error has occurred running command: %s' % cmd, file=sys.stderr)
        sys.exit(proc.returncode)
    return out


def cmd_out(cmd):
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=True,
        executable='/bin/bash',
    )

    out, _ = proc.communicate()

    return out


def cmd_ignore_error(cmd):
    """Executes a subprocess running a shell command; reports but ignores any errors."""
    proc = subprocess.Popen(cmd, shell=True, executable='/bin/bash')
    proc.communicate()

    if proc.returncode:
        print('Error occurred running host command: %s' % cmd, file=sys.stderr)
        print('Ignored, process continues..')
        return proc.returncode


def cmd_check_success(cmd):
    """Executes a subprocess running a shell command; checks its run."""
    try:
        subprocess.check_call(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=True,
            executable='/bin/bash', )
        return True
    except subprocess.CalledProcessError:
        return False


def main_with_cmds(cmds={}, add_arguments=None):
    def _print_usage(parser):
        parser.print_help(file=sys.stderr)
        sys.exit(2)

    parser = argparse.ArgumentParser(description='Collector cmds.')

    cmds.update({
        'argtest': lambda: print("halo, arg arg."),
        'help': lambda: _print_usage(parser),
    })

    for name in list(cmds.keys()):
        if '_' in name:
            cmds[name.replace('_', '-')] = cmds[name]

    cmdlist = sorted(cmds.keys())

    parser.add_argument(
        'action',
        metavar='action',
        nargs='?',
        default='help',
        choices=cmdlist,
        help='Action is one of ' + ', '.join(cmdlist))

    parser.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        help='enable verbose')

    if add_arguments:
        add_arguments(parser)

    args = parser.parse_args()

    if args.verbose:
        os.environ['V'] = '1'
    cmds[args.action]()


if __name__ == '__main__':
    print(get_worker_instance_types_static())
