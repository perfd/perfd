from .util import main_with_cmds
from .util import k8s

from . import remote as rt
from . import tc
import pprint as pp


# tests
def test():
    host_ips = rt.worker_ips()
    rt.install_utils(host_ips)

    tc.clear_tc_rules(host_ips)
    # tc.add_delay_remote(host_ips, 100)
    tc.add_rate_limit_remote(host_ips, 100)
    tc.add_loss_remote(host_ips, 10)
    tc.validate_changes(host_ips)
    print("tc: done.")


if __name__ == '__main__':
    cmds = {
        "list": lambda: pp.pprint(rt.worker_ips()),
        "map": lambda: pp.pprint(k8s.get_worker_external_internal_ip_map()),
        "install": lambda: rt.install_utils(rt.worker_ips()),
        "clear": lambda: tc.clear_tc_rules(rt.worker_ips()),
        "rate": tc.add_rate_limit_remote,
        "delay": tc.add_delay_remote,
        "loss": tc.add_loss_remote,
        "test": test,
    }

    main_with_cmds(cmds)
