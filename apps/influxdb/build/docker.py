import random
from apps.util.dockerhub import get_tags

# Note these are non-enterprise versions
staged_tags = ['latest', '0.12', '0.12.2', '0.13', '0.13-alpine', '0.13.0', '0.13.0-alpine', '1.0', '1.0-alpine',
               '1.0.0', '1.0.0-alpine', '1.0.0-beta1', '1.0.0-beta1-alpine', '1.0.0-beta2', '1.0.0-beta2-alpine',
               '1.0.0-beta3', '1.0.0-beta3-alpine', '1.0.0-rc1', '1.0.0-rc1-alpine', '1.0.0-rc2', '1.0.0-rc2-alpine',
               '1.0.1', '1.0.1-alpine', '1.0.2', '1.0.2-alpine', '1.1', '1.1-alpine', '1.1.0', '1.1.0-alpine',
               '1.1.0-rc1', '1.1.0-rc1-alpine', '1.1.0-rc2', '1.1.0-rc2-alpine', '1.1.1', '1.1.1-alpine', '1.1.2',
               '1.1.2-alpine', '1.1.3', '1.1.3-alpine', '1.1.4', '1.1.4-alpine', '1.1.5', '1.1.5-alpine', '1.2',
               '1.2-alpine', '1.2.0', '1.2.0-alpine', '1.2.0-rc1', '1.2.0-rc1-alpine', '1.2.0-rc2', '1.2.0-rc2-alpine',
               '1.2.1', '1.2.1-alpine', '1.2.2', '1.2.2-alpine', '1.2.3', '1.2.3-alpine', '1.2.4', '1.2.4-alpine',
               '1.3', '1.3-alpine', '1.3.0', '1.3.0-alpine', '1.3.1', '1.3.1-alpine', '1.3.2', '1.3.2-alpine', '1.3.3',
               '1.3.3-alpine', '1.3.4', '1.3.4-alpine', '1.3.5', '1.3.5-alpine', '1.3.6', '1.3.6-alpine', '1.3.7',
               '1.3.7-alpine', '1.3.8', '1.3.8-alpine', '1.3.9', '1.3.9-alpine', '1.4', '1.4-alpine', '1.4.0',
               '1.4.0-alpine', '1.4.1', '1.4.1-alpine', '1.4.2', '1.4.2-alpine', '1.4.3', '1.4.3-alpine', '1.5',
               '1.5-alpine', '1.5.0', '1.5.0-alpine', '1.5.1', '1.5.1-alpine', '1.5.2', '1.5.2-alpine', '1.5.3',
               '1.5.3-alpine', '1.5.4', '1.5.4-alpine', '1.6', '1.6-alpine', '1.6.0', '1.6.0-alpine', '1.6.1',
               '1.6.1-alpine', '1.6.2', '1.6.2-alpine', '1.6.3', '1.6.3-alpine', '1.6.4', '1.6.4-alpine', '1.6.5',
               '1.6.5-alpine', '1.6.6', '1.6.6-alpine', '1.7', '1.7-alpine', '1.7.0', '1.7.0-alpine', '1.7.1',
               '1.7.1-alpine', '1.7.10', '1.7.10-alpine', '1.7.2', '1.7.2-alpine', '1.7.3', '1.7.3-alpine', '1.7.4',
               '1.7.4-alpine', '1.7.5', '1.7.5-alpine', '1.7.6', '1.7.6-alpine', '1.7.7', '1.7.7-alpine', '1.7.8',
               '1.7.8-alpine', '1.7.9', '1.7.9-alpine', '1.8', '1.8-alpine', '1.8.0', '1.8.0-alpine', 'alpine']

staged_tags_small = ['1.4', '1.4-alpine', '1.4.0', '1.4.0-alpine', '1.4.1', '1.4.1-alpine', '1.4.2', '1.4.2-alpine',
                     '1.4.3', '1.4.3-alpine', '1.5', '1.5-alpine', '1.5.0', '1.5.0-alpine', '1.5.1', '1.5.1-alpine',
                     '1.5.2', '1.5.2-alpine', '1.5.3', '1.5.3-alpine', '1.5.4', '1.5.4-alpine', '1.6', '1.6-alpine',
                     '1.6.0', '1.6.0-alpine', '1.6.1',
                     '1.6.1-alpine', '1.6.2', '1.6.2-alpine', '1.6.3', '1.6.3-alpine', '1.6.4', '1.6.4-alpine', '1.6.5',
                     '1.6.5-alpine', '1.6.6', '1.6.6-alpine', '1.7', '1.7-alpine', '1.7.0', '1.7.0-alpine', '1.7.1',
                     '1.7.1-alpine', '1.7.10', '1.7.10-alpine', '1.7.2', '1.7.2-alpine', '1.7.3', '1.7.3-alpine',
                     '1.7.4', '1.7.4-alpine', '1.7.5', '1.7.5-alpine', '1.7.6', '1.7.6-alpine', '1.7.7', '1.7.7-alpine',
                     '1.7.8', '1.7.8-alpine', '1.7.9', '1.7.9-alpine', '1.8', '1.8-alpine', '1.8.0', '1.8.0-alpine']


def tags(filter_enterprise=True):
    ts = get_tags("influxdb")
    if filter_enterprise:
        ts = [t for t in ts if "meta" not in t and "data" not in t]
    return ts


if __name__ == "__main__":
    # print(tags())
    random.seed(42)
    random.shuffle(staged_tags_small)
    print(staged_tags_small)
