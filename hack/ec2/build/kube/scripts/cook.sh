#!/bin/bash

# Create and start a new, fission-ready kube cluster

source env.exp

# First-time use: set up iam, create s3 bucket
# ./kubectl.sh
# ./kops-iam.sh
# ./s3-state-store.sh
# ./helm-cli.sh

cluster.sh
start.sh

#kubectl config set-context $NAME
#
#while true; do
#    kops validate cluster
#    if [ "$?" -ne 0 ]; then
#        echo "-> waiting for ec2 instances ready...";
#        sleep 25
#        continue;
#    fi
#    break
#done
#
#echo "-> cluster $NAME ready."

# install helm server
helm-server.sh

# install fission
sleep 15
echo "-> waiting for helm-tiller ready."
fission.sh




