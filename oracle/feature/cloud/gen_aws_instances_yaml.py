import os
import csv
import yaml
from operator import itemgetter

from yaml.representer import Representer
from collections import OrderedDict, defaultdict

import oracle.utils as oc_utils
import oracle.feature.cloud.utils.value_converter as vc
import oracle.feature.cloud.utils.aws_ins_family as awsf

_dir_path = os.path.dirname(os.path.realpath(__file__))
_input_file = _dir_path + "/manifests/" + "aws-latest.csv"
_output_file = _dir_path + "/manifests/" + "aws-latest.yaml"
_kops_type_file = _dir_path + "/manifests/" + "aws-kops-machine-types.go"


def represent_ordereddict(dumper, data):
    """Stackoverflow: https://stackoverflow.com/a/16782282."""
    value = []

    for item_key, item_value in data.items():
        node_key = dumper.represent_data(item_key)
        node_value = dumper.represent_data(item_value)

        value.append((node_key, node_value))

    return yaml.nodes.MappingNode(u'tag:yaml.org,2002:map', value)


def gen(input_file: str = _input_file, output_file: str = _output_file,
        *,
        readable: bool = False,
        select: callable = None):
    """
    format:

    metadata:
        creationTimestamp:
    instanceTypes:
        attribute: value
        ...

    note:
        - all space of attributes' and values' will be
        replaced by an underscore.
        - order of attributes will be preserved.
    """
    yaml.add_representer(OrderedDict, represent_ordereddict)
    yaml.add_representer(defaultdict, Representer.represent_dict)

    info = OrderedDict({
        "metadata": {
            "creationTimestamp": oc_utils.utc_now_str(),
        },
        "instanceTypes": defaultdict(OrderedDict),
    })

    count = 0
    with open(input_file, mode="r") as csv_file:
        csv_reader = csv.DictReader(csv_file)

        for i, row in enumerate(csv_reader):
            ins_name = row["API Name"]

            if select is not None and not select(ins_name):
                continue

            for k, v in row.items():
                info["instanceTypes"][ins_name][vc.normalized_key(k, readable)] = vc.normalized_value(v, readable)

            count += 1

    with open(output_file, mode="w") as result_file:
        blob = yaml.dump(info, default_flow_style=False)
        result_file.write(blob)

    print("done.", "written in", output_file, ".", "total instance types:", count)


def get_kops_support_type():
    ins_types = set()
    with open(_kops_type_file, "r") as f:
        for line in f.readlines():
            if "Name:" in line:
                ins_type = line.split()[1].replace("\"", "").replace(",", "")
                ins_types.add(ins_type)
    print("done. total %d kops supported aws instance types" % len(ins_types))
    return ins_types


def make_select_common_func():
    family_set = set().union(*[
        awsf.general_family,
        awsf.general_burstable_family,
        awsf.compute_family,
        awsf.compute_freq_family,
        awsf.memory_family,
        awsf.diskio_family,
    ])

    scale_set = set().union(*[
        awsf.small_scales,
        awsf.medium_scales,
        awsf.large_scales,
    ])

    kops_support_set = get_kops_support_type()

    def func(ins_name: str) -> bool:
        family, scale = itemgetter(0, 1)(ins_name.split("."))
        if family in family_set and scale in scale_set and ins_name in kops_support_set:
            return True

    return func


def make_select_common_latest_func():
    family_set = set().union(*[
        awsf.general_family_latest,
        awsf.compute_family_latest,
        awsf.memory_family_latest,
    ])

    scale_set = set().union(*[
        awsf.small_scales,
        awsf.medium_scales,
        awsf.large_scales,
    ])

    # TODO: plug this out
    family_set_black_list = {
        "m5a", "r5a", "c1", "m1",
    }

    ins_name_black_list = {
        "m3.medium", "m3.large", "c5d.xlarge", "r5d.large"
    }

    kops_support_set = get_kops_support_type()

    def func(ins_name: str) -> bool:
        family, scale = itemgetter(0, 1)(ins_name.split("."))
        if (family in family_set and family not in family_set_black_list) \
                and scale in scale_set \
                and ins_name in kops_support_set \
                and ins_name not in ins_name_black_list:
            return True

    return func


def make_select_common_small_func():
    family_set = set().union(*[
        awsf.general_family,
        awsf.compute_family,
        awsf.memory_family,
    ])

    scale_set = set().union(*[
        awsf.small_scales,
        awsf.medium_scales,
        awsf.large_scales,
    ])

    # TODO: plug this out
    family_set_black_list = {
        "m5a", "r5a", "c1", "m1",
    }

    ins_name_black_list = {
        "m3.medium", "m3.large", "c5d.xlarge", "r5d.large"
    }

    kops_support_set = get_kops_support_type()

    def func(ins_name: str) -> bool:
        family, scale = itemgetter(0, 1)(ins_name.split("."))
        if (family in family_set and family not in family_set_black_list) \
                and scale in scale_set \
                and ins_name in kops_support_set \
                and ins_name not in ins_name_black_list:
            return True

    return func


def make_select_common_tiny_func(family_set=None):
    if family_set is None:
        family_set = set().union(*[
            awsf.general_family_latest,
            awsf.compute_family_latest,
            awsf.memory_family_latest,
        ])

    scale_set = set().union(*[
        awsf.small_scales,
    ])

    # TODO: plug this out
    family_set_black_list = {
        "m5a", "r5a", "c1", "m1",
    }

    ins_name_black_list = {
        "m3.medium", "m3.large", "c5d.xlarge", "r5d.large"
    }

    kops_support_set = get_kops_support_type()

    def func(ins_name: str) -> bool:
        family, scale = itemgetter(0, 1)(ins_name.split("."))
        if (family in family_set and family not in family_set_black_list) \
                and scale in scale_set \
                and ins_name in kops_support_set \
                and ins_name not in ins_name_black_list:
            return True

    return func


def make_select_common_tiny_func_1_2():
    make_select_common_tiny_func()


def make_select_structured_func():
    """TODO: select the set of instances that cover the cpu x mem x disk x network space"""
    pass


if __name__ == '__main__':
    # gen(output_file=_dir_path + "/instances/" + "aws-common.yaml", select=make_select_common_func())
    # gen(output_file=_dir_path + "/manifests/" + "aws-common-tiny-generic.yaml", select=make_select_common_tiny_func(
    #     set().union(*[
    #         awsf.general_family_latest,
    #     ])
    # ))
    # gen(output_file=_dir_path + "/manifests/" + "aws-common-tiny-comp.yaml", select=make_select_common_tiny_func(
    #     set().union(*[
    #         awsf.compute_family_latest,
    #     ])
    # ))
    # gen(output_file=_dir_path + "/manifests/" + "aws-common-tiny-mem.yaml", select=make_select_common_tiny_func(
    #     set().union(*[
    #         awsf.memory_family_latest,
    #     ])
    # ))

    gen(output_file=_dir_path + "/manifests/" + "aws-common-latest.yaml", select=make_select_common_latest_func())
