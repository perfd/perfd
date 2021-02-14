import os
import yaml
import time
import pprint as pp

import hack.ec2.build.kube.utils as utils

_dir_path = os.path.dirname(os.path.realpath(__file__))
_script_dir = _dir_path + "/scripts/"
_template_dir = _dir_path + "/ig_gen/"
_env_file = _script_dir + "env.exp"

"""This module currently supports only updating the numbers of existing kops instance groups."""


def autoscale(ins_groups, *args, **kwargs):
    scale_to_multiple({
        ig["instanceType"]: ig["numInstance"] for ig in ins_groups
    }, *args, **kwargs)


def scale_to(ins_type, ins_num, update=True):
    """Scale to an objective of one instance type; blocking until done.

    TODO: move to using scale_to_multiple.
    TODO: check if asynchronous, individual scale down is possible."""

    ig_config = get_ig_config(get_ig_name(ins_type))
    if "spec" not in ig_config:
        print("k8s autoscaler: no such instance group, creating one...")
        create_ig(ins_type, ins_num)

        if update:
            update_cluster()
            validate(ins_type, ins_num)
    else:
        ins_cur_num = get_ins_num(ins_type, ig_config)
        if ins_cur_num != ins_num:
            # update the instance count
            ig_config["spec"]["minSize"] = ins_num
            ig_config["spec"]["maxSize"] = ins_num

            # replace the kops state file
            replace_ig(ig_config)

            if update:
                update_cluster()
                validate(ins_type, ins_num)
        else:
            print("k8s autoscaler: already met")
    if update:
        print("k8s autoscaler: done scaling {} -> {}".format(ins_type, ins_num))


def scale_to_multiple(ins_type_num: dict, scale_down_other_ig=True, always_update=False):
    """Scale to an objective of multiple instance types and numbers; blocking until done."""
    cur_igs = get_all_worker_ig_num()

    need_update = always_update
    for ins_type, ins_num in ins_type_num.items():
        ig_name = get_ig_name(ins_type)
        print("current instance groups:", cur_igs)
        if ig_name not in cur_igs:
            print("k8s autoscaler: no such instance group, creating one...")
            create_ig(ins_type, ins_num)
            need_update = True
        else:
            ins_cur_num = cur_igs[ig_name]

            if ins_cur_num != ins_num:
                ig_config = get_ig_config(get_ig_name(ins_type))

                # update the instance count
                ig_config["spec"]["minSize"] = ins_num
                ig_config["spec"]["maxSize"] = ins_num

                # replace the kops state file
                print("k8s autoscaler:", ins_type, ins_num, "replacing..")
                replace_ig(ig_config)

                need_update = True
            else:
                print("k8s autoscaler: {} already met".format((ins_type, ins_num)))

    if scale_down_other_ig:
        for ig, num in cur_igs.items():
            ins_type = get_ins_type_name(ig)
            if ins_type not in ins_type_num:
                scale_to(ins_type, 0, update=False)  # delay the update later
                need_update = True

    if need_update:
        update_cluster()
        validate_multiple(ins_type_num)
        print("k8s autoscaler: done scaling:")
        pp.pprint(ins_type_num)
    else:
        print("k8s autoscaler: already met, no updates.")


def update_cluster():
    # update the cluster
    utils.cmd("source {}; kops update cluster --yes".format(_env_file))


def validate(ins_type, ins_num):
    counter, interval, max_trials = 0, 10, 100
    while get_ins_ready_num(ins_type) != ins_num and counter < max_trials:
        time.sleep(interval)
        counter += 1
        print("k8s autoscaler: {}th validation for {} -> {}...".format(counter, ins_type, ins_num))
    if counter == max_trials:
        raise Exception(
            "k8s autoscaler: Max trial exceeded, unable to scale the instance group: {} -> {}".format(ins_type,
                                                                                                      ins_num))
    print("k8s autoscaler: passed.")
    return True


def validate_multiple(ins_type_num):
    for ins_type, ins_num in ins_type_num.items():
        if not validate(ins_type, ins_num):
            return False
    return True


def get_ins_num(ins_type: str, config: dict = None):
    """Return the instance number in the instance group. Note that this number may not reflect the actual
    ready nodes of this instance type."""
    ig_name = get_ig_name(ins_type)
    config = config if config else get_ig_config(ig_name)
    min_size, max_size = config["spec"]["minSize"], config["spec"]["maxSize"]

    assert config["metadata"]["name"] == ig_name
    assert min_size == max_size, "they should be equal unless you want your kops to do autoscaling"
    return min_size


def get_ins_ready_num(ins_type: str):
    # TODO: more reliable counting ready nodes
    return utils.cmd_out("source {}; kubectl get nodes -l \"kops.k8s.io/instancegroup={}\" "
                         .format(_env_file, get_ig_name(ins_type))).decode("utf-8").count(" Ready")


def get_ig_config(ig_name: str, raw_str: str = None) -> dict:
    out_str = raw_str if raw_str else get_ig_config_raw(ig_name)
    config = yaml.load(out_str)
    return config


def get_ig_config_raw(ig_name: str):
    return utils.cmd_out("source {};  kops get ig {} --name $NAME --output yaml"
                         .format(_env_file, ig_name)).decode("utf-8")


def get_ig_name(ins_type: str):
    """Returns the default instance group name, this currently works with only worker nodes."""
    return "nodes." + ins_type


def get_cluster_info():
    ci_raw = utils.cmd_out("source {}; kops get cluster --output yaml"
                           .format(_env_file)).decode("utf-8")
    ci = yaml.load(ci_raw)

    return {
        "name": ci["metadata"]["name"],
        "region": ci["spec"]["subnets"][0]["zone"],  # TODO: support multiple zones
    }


def get_ins_type_name(ig: str):
    return ig.replace("nodes.", "")


def get_all_worker_ig_num():
    docs = utils.cmd_out("source {};  kops get ig --output yaml"
                         .format(_env_file)).decode("utf-8")
    igs = dict()
    for doc in yaml.load_all(docs):
        assert doc["spec"]["maxSize"] == doc["spec"]["minSize"]
        name = doc["metadata"]["name"]
        if name.startswith("master") or name.endswith("prom"):
            continue

        num = doc["spec"]["maxSize"]
        igs[name] = num
    return igs


def create_ig(ins_type, ins_num):
    # ig_config = utils.yaml_load(_template_dir + "ig_template.yaml")
    ci = get_cluster_info()

    with open(_template_dir + "/ig_template.yaml", "r") as f:
        # TODO: the .k8s.local and region "a" postfix should be handle else where
        ig_spec = f.read().replace("{{name}}", ci["name"]
                                   .replace(".k8s.local", "")).replace("{{region}}", ci["region"].rstrip("a"))

    ig_config = utils.yaml_loads(ig_spec)

    ig_name = get_ig_name(ins_type)
    ig_config["metadata"]["name"] = ig_name

    ig_config["spec"]["machineType"] = ins_type
    ig_config["spec"]["nodeLabels"]["kops.k8s.io/instancegroup"] = ig_name
    ig_config["spec"]["maxSize"] = ins_num
    ig_config["spec"]["minSize"] = ins_num

    upload_ig(ig_config, cmd="create".format(name=ig_name))


def replace_ig(ig_config: dict):
    upload_ig(ig_config, "replace")


def upload_ig(ig_config: dict, cmd):
    temp_file = "/tmp/" + utils.uuid_str()
    with open(temp_file, "w") as f:
        f.write(utils.yaml_dump_str(ig_config))
    utils.cmd("source {}; kops {} -f {}".format(_env_file, cmd, temp_file))
    utils.cmd("rm " + temp_file)


if __name__ == '__main__':
    # utils.cmd("source " + _env_file)
    # scale_to("m4.4xlarge", 0)
    # import pprint as pp

    # pp.pprint(create_ig("m4.2xlarge", 1))
    # print(get_all_worker_ig_num())
    print(get_cluster_info())
