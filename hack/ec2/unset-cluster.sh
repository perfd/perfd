#!/usr/bin/env bash

CLUSTERID=$1

kubectl config unset users.${CLUSTERID}
kubectl config unset contexts.${CLUSTERID}
kubectl config unset clusters.${CLUSTERID}
