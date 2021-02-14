#!/bin/bash

# if kubectl not installed: ./kubectl.sh
command -v kubectl >/dev/null 2>&1 || { kubectl.sh; }
kubectl version || { echo 'kubectl failed, is it correctly installed?' ; exit 1; }

# install helm cli

curl -LO https://storage.googleapis.com/kubernetes-helm/helm-v2.6.1-linux-amd64.tar.gz
tar xzf helm-v2.6.1-linux-amd64.tar.gz
sudo mv linux-amd64/helm /usr/local/bin

# clean up
rm helm*.tar.gz
rm -rf linux-amd64/
