from . import remote as rt


class Tcpdump:
    def __init__(self, ip, loc, *, arg_str=""):
        self.ip = ip
        self.loc = loc
        self.cap_file = ""
        self.arg_str = arg_str

    def __enter__(self):
        self._kill()

        cmd_ = "mktemp"
        self.cap_file = rt.cmd_remote(host_ips=[self.ip],
                                      cmd_=cmd_,
                                      out=True)[0].decode().rstrip()

        print("tcpdump: storing trace in %s on host %s" % (self.cap_file, self.ip))

        cmd_ = "sudo tcpdump %s -w %s" % (self.arg_str, self.cap_file)
        rt.cmd_remote(host_ips=[self.ip],
                      cmd_=cmd_,
                      out=False,
                      async_=True)

    def __exit__(self, type, value, traceback):
        self._kill()

        rt.scp(host_ips=[self.ip],
               src_path=self.cap_file,
               dest_path=self.loc)

        print("tcpdump: fetched trace in %s" % self.loc)

        cmd_ = "sudo rm %s" % self.cap_file
        rt.cmd_remote(host_ips=[self.ip],
                      cmd_=cmd_,
                      out=False)

    def _kill(self):
        cmd_ = "sudo pkill tcpdump"
        rt.cmd_remote(host_ips=[self.ip],
                      cmd_=cmd_,
                      out=True)


class Dumb:
    def __enter__(self):
        pass

    def __exit__(self, type, value, traceback):
        pass
