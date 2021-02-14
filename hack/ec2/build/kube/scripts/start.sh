#!/bin/bash

source env.exp

# default availability zone is zone 'a'

kops update cluster $NAME --yes

kubectl config set-context $NAME

# when the kops support for current version is added, remove the use of this counter
counter=0
trials=20
while true; do
    if [ $counter -eq $trials ]; then
        break
    fi
    counter=$((counter + 1))

    kops validate cluster
    if [ "$?" -ne 0 ]; then
        echo "-> waiting for ec2 instances ready...";
        sleep 25
        continue;
    fi
    break
done

echo "-> cluster $NAME ready."

