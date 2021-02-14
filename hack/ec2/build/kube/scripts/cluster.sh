#!/usr/bin/env bash

source env.exp

# default availability zone is zone 'a', change it as you may

#kops create cluster --zones ${AWS_REGION}a --kubernetes-version https://${KUBE_BINARY_STORE_NAME}.s3.amazonaws.com/kubernetes/dev/${KUBE_VERSION}/ $NAME
kops create -f spec.k8s.local.yaml
kops create secret --name ${NAME} sshpublickey admin -i ~/.ssh/id_rsa.pub
