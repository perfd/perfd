import os
import yaml
import shutil
import datetime

import hack.ec2.build.kube.utils as utils

from hack.ec2.build.kube.ig_gen import configs

_dir_path = os.path.dirname(os.path.realpath(__file__))
_template_file = _dir_path + "/ig_template.yaml"
_base_file = _dir_path + "/ig_base.yaml"
_result_file = _dir_path + "/foo.k8s.local.yaml"  # TODO: move this to ...
_source_file = _dir_path + "/../scripts/foo.k8s.local.yaml"


class NoDatesSafeLoader(yaml.SafeLoader):
    @classmethod
    def remove_implicit_resolver(cls, tag_to_remove):
        """
        Remove implicit resolvers for a particular tag.

        Takes care not to modify resolvers in super classes.

        Example: we want to load datetimes as strings, not dates, because we
        go on to serialise as json which doesn't have the advanced types
        of yaml, and leads to incompatibilities down the track.

        This code snippet is adapted from Damien Ayers's stackoverflow answer.
        """
        if not 'yaml_implicit_resolvers' in cls.__dict__:
            cls.yaml_implicit_resolvers = cls.yaml_implicit_resolvers.copy()

        for first_letter, mappings in cls.yaml_implicit_resolvers.items():
            cls.yaml_implicit_resolvers[first_letter] = [(tag, regexp)
                                                         for tag, regexp in mappings
                                                         if tag != tag_to_remove]


# Remove the timestamp parsing as kubernetes's format is not used by default for the json.dump
# NoDatesSafeLoader.remove_implicit_resolver('tag:yaml.org,2002:timestamp')


def gen_block(ins_type, ins_num,
              region="us-west-2", name="foo", *,
              image="238764668013/k8s-worker",
              tenancy="default"):
    """Generate an ig block for given ig parameters."""
    with open(_template_file, "r") as f:
        f = f.read().replace("{{name}}", name)\
            .replace("{{region}}", region)

        block = yaml.safe_load(f)
        block["metadata"]["name"] = "nodes." + ins_type
        block["spec"]["image"] = image
        block["spec"]["tenancy"] = tenancy
        block["spec"]["machineType"] = ins_type
        block["spec"]["maxSize"] = ins_num
        block["spec"]["minSize"] = ins_num
        block["spec"]["nodeLabels"]["kops.k8s.io/instancegroup"] = "nodes." + ins_type
        return utils.yaml_dump_str(block)


def gen_file(ins_type_num: dict):
    shutil.copyfile(_base_file, _result_file)
    with open(_result_file, "a") as f:
        out_str = ""
        for ins_type, num in ins_type_num.items():
            out_str += "\n---\n\n"
            out_str += gen_block(ins_type, num)
        f.write(out_str)
        return out_str


def apply():
    shutil.copy(_result_file, _source_file)
    print("updated instance groups yaml.")


def gen_and_apply(ins_type_num: dict = configs.ins_type_num):
    gen_file(ins_type_num)
    apply()


def main():
    from build.kube.utils import main_with_cmds
    cmds = {
        "gen": lambda: gen_and_apply(),
    }
    main_with_cmds(cmds)


if __name__ == '__main__':
    main()
