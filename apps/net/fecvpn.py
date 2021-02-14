from . import remote as rt

_binary_url = "https://github.com/wangyu-/tinyfecVPN/releases/download/20180820.0/tinyvpn_binaries.tar.gz"


class FecVPN:
    def __init__(self, server, *,
                 port=8091, passwd="",
                 subnet="10.22.22.0",
                 fecratio="20:10",
                 nodes=None):
        self.server = server
        self.port = port
        self.passwd = passwd

        self.subnet = subnet
        self.fecratio = fecratio

        self.nodes = list() if nodes is None else nodes
        self.install([self.server] + self.nodes)

    @staticmethod
    def install(host):
        # TODO:
        pass

    def start(self):
        # TODO:
        pass

    def join(self, host):
        # TODO:
        pass

    def stop(self):
        # TODO:
        for n in [self.server] + self.nodes:
            pass


if __name__ == '__main__':
    pass
