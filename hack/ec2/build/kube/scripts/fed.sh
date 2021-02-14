#!/usr/bin/env bash

create()
{
    kubectl create ns kube-multicluster-public
    helm install charts/federation-v2 --name federation-v2 --namespace federation-system
}

delete()
{
    kubectl -n federation-system delete FederatedTypeConfig --all
    kubectl delete crd $(kubectl get crd | grep -E 'federation.k8s.io' | awk '{print $1}')
    kubectl delete crd clusters.clusterregistry.k8s.io
    helm delete --purge federation-v2
    kubectl delete ns federation-system
    kubectl delete ns kube-multicluster-public
}