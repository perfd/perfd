#!/bin/bash

source env.exp

kops delete secret sshpublickey admin
kops create secret sshpublickey admin -i ~/.ssh/id_rsa.pub
kops update cluster --yes
kops rolling-update cluster --yes
