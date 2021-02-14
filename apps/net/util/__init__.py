import os
import sys
import argparse
import subprocess
import uuid


def uuid_str():
    return str(uuid.uuid4())


def which(program):
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file
    return None


def main_with_cmds(cmds, add_arguments=None):
    def _print_usage(p):
        p.print_help(file=sys.stderr)
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


def cmd(cmd, out=False, quiet=False, async_=False, executable='/bin/bash'):
    """Executes a subprocess running a shell command and returns the output."""
    if async_:
        subprocess.Popen(cmd,
                         shell=True,
                         stdin=None,
                         stdout=None,
                         stderr=None,
                         close_fds=True,
                         executable=executable)
        return
    elif quiet or out:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=True,
            executable=executable)
    else:
        proc = subprocess.Popen(cmd, shell=True)

    if proc.returncode:
        if quiet:
            print('Log:\n', out, file=sys.stderr)
        print('Error has occurred running command: %s' % cmd, file=sys.stderr)
        sys.exit(proc.returncode)
    out, _ = proc.communicate()
    return out
