import sys
import subprocess
import uuid


def uuid_str():
    return str(uuid.uuid4())


def exit_str(code=0, msg=None):
    if msg is not None:
        print(msg)
    exit(code)


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
