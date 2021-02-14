#!/usr/bin/env bash

VERSION=0.1.0-rc6

until helm repo add kubefed-charts https://raw.githubusercontent.com/kubernetes-sigs/kubefed/master/charts
do
  echo "waiting for tiller to be ready.."
  sleep 2
done

helm install kubefed-charts/kubefed --name kubefed --version=${VERSION} --namespace kube-federation-system

echo "kubefed installed."