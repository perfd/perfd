from .remote import *


def install_dns_utils(host_ips: list):
    cmd = "sudo apt install host -y &"
    cmd_remote(host_ips, cmd, async_=True)
    time.sleep(5)


# tests
def test():
    host_ips = worker_ips()
    install_dns_utils(host_ips)


if __name__ == '__main__':
    test()
