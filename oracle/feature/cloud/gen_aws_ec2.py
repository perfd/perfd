import os
import csv
import oracle.feature.cloud.utils.value_converter as vc
import numpy as np
from sklearn import preprocessing

import yaml

# static = {
#     "API Name",
#     "Arch",
#     "Compute Units (ECU)",
#     "EBS Optimized: Max 16K IOPS",
#     "EBS Optimized: Max Bandwidth",
#     "EBS Optimized: Throughput",
#     "ECU per vCPU",
#     "Enhanced Networking",
#     "FPGAs",
#     "GPUs",
#     "IPv6 Support",
#     "Instance Storage",
#     "Instance Storage: SSD TRIM Support",
#     "Instance Storage: already warmed-up",
#     "Linux Virtualization",
#     "Max ENIs",
#     "Max IPs",
#     "Memory",
#     "Network Performance",
#     "On EMR",
#     "Placement Group Support",
#     "VPC Only",
#     "vCPUs",
#     "num_instance",
#     "jct",
# }

static = {'API_Name', 'Arch', 'Clock_Speed(GHz)', 'Compute_Units_(ECU)', 'EBS_Exposed_as_NVMe',
          'EBS_Optimized__Max_16K_IOPS',
          'EBS_Optimized__Max_Bandwidth', 'EBS_Optimized__Throughput', 'EBS_Optimized_surcharge', 'ECU_per_vCPU',
          'EMR_cost',
          'Enhanced_Networking', 'FPGAs', 'GPUs', 'IPv6_Support', 'Instance_Storage',
          'Instance_Storage__SSD_TRIM_Support',
          'Instance_Storage__already_warmed-up', 'Intel_AVX', 'Intel_AVX2', 'Intel_Turbo', "num_executor",
          "dataset_size"}


def gen_resource_map_old(resource_only=True) -> dict:
    # TODO: use the yaml representation to build this map
    resource_map = dict()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    file_name = "aws-resource-only.csv" if resource_only else "aws.csv"
    with open(dir_path + "/manifests/" + file_name, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for i, row in enumerate(csv_reader):
            if i == 0:
                continue
    return resource_map


def gen_resource_map() -> dict:
    dir_path = os.path.dirname(os.path.realpath(__file__))
    file_name = "aws-common.yaml"
    with open(dir_path + "/manifests/" + file_name, mode='r') as stream:
        info = yaml.safe_load(stream)["instanceTypes"]
    return info


aws_resource_map = gen_resource_map()

resource_map = aws_resource_map
# TODO: add label encoder for non-numeric feature
# {"API_Name", "Arch", "Instance_Storage"}
resource_filter = {'Clock_Speed(GHz)', 'Compute_Units_(ECU)', 'EBS_Exposed_as_NVMe',
                   'EBS_Optimized:_Max_16K_IOPS',
                   'EBS_Optimized:_Max_Bandwidth', 'EBS_Optimized:_Throughput', 'EBS_Optimized_surcharge',
                   'ECU_per_vCPU',
                   'EMR_cost',
                   'Enhanced_Networking', 'FPGAs', 'GPUs', 'IPv6_Support',
                   'Instance_Storage:_SSD_TRIM_Support',
                   'Instance_Storage:_already_warmed-up', 'Intel_AVX', 'Intel_AVX2', 'Intel_Turbo'}


def gen_ins_type_label_encoder(ins_types: list = list(set(aws_resource_map.keys()))):
    le = preprocessing.LabelEncoder()
    ins_types = np.array(sorted(ins_types))

    le.fit(ins_types)
    return le


aws_instance_label_encoder = gen_ins_type_label_encoder()

if __name__ == '__main__':
    import pprint as pp

    test_ins = ["c4.4xlarge", "m4.large"]
    pp.pprint({
        k: v for k, v in zip(test_ins, aws_instance_label_encoder.transform(test_ins))
    })

    # pprint.pprint(gen_resource_map())
