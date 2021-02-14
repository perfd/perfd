from .remote import cmd_remote


def update_tc_rule(host_ips: list, rule):
    cmd = "sudo " + rule + " &"
    cmd_remote(host_ips, cmd)


def clear_tc_rules(host_ips: list):
    rule = "tc qdisc del dev eth0 root"
    update_tc_rule(host_ips, rule)


# traffic rules
def add_rate_limit_remote(host_ips, rate: int = 1024):
    """Add rate limit in mbps, using token buffer filter by default."""
    rate = rate / 8
    rule = "sudo tc qdisc add dev eth0 handle 1: root htb default 11;" \
           "sudo tc class add dev eth0 parent 1: classid 1:1 htb rate {rate}mbps;" \
           "sudo tc class add dev eth0 parent 1:1 classid 1:11 htb rate {rate}mbps".format(rate=str(float(rate)))

    update_tc_rule(host_ips, rule)


def add_loss_remote(host_ips, loss: int = 0):
    """Add loss in percentage."""
    rule = "tc qdisc add dev eth0 root netem loss {}%".format(loss)
    update_tc_rule(host_ips, rule)


def add_delay_remote(host_ips, delay: int = 0):
    """Add delay in milliseconds."""
    rule = "tc qdisc add dev eth0 root netem delay {}ms".format(delay)
    update_tc_rule(host_ips, rule)


def add_rate_delay_loss_remote(host_ips, rate, delay, loss):
    """TODO: fix this"""
    rate = rate / 8
    rule = "sudo tc qdisc add dev eth0 handle 1: root htb default 11;" \
           "sudo tc class add dev eth0 parent 1: classid 1:1 htb rate {rate}mbps;" \
           "sudo tc class add dev eth0 parent 1:1 classid 1:11 htb rate {rate}mbps".format(rate=str(float(rate)))
    update_tc_rule(host_ips, rule)

    rule = "tc qdisc add dev eth0 root netem delay {delay}ms loss {loss}%".format(
        delay=delay,
        loss=loss,
    )
    update_tc_rule(host_ips, rule)


# def add_rate_limit_more_remote(host_ips, rate, burst, drop_latency):
#     """Add rate limit in mbit, burst in kbit, drop_latency in ms
#
#     tbf: use the token buffer filter to manipulate traffic rates (hard-coded default)
#     rate: sustained maximum rate
#     burst: maximum allowed burst
#     drop_latency: packets with higher latency get dropped
#     """
#     pass


def validate_changes(host_ips):
    cmd_remote(host_ips, "sudo tc class show dev eth0")