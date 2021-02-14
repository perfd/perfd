#!/usr/bin/env bash

source env.exp

istio=../../../thirdparty/istio-0.7.1

# Install istio
kubectl apply -f ${istio}/install/kubernetes/istio.yaml

# CA configs for sidecar injection
${istio}/install/kubernetes/webhook-create-signed-cert.sh \
    --service istio-sidecar-injector \
    --namespace istio-system \
    --secret sidecar-injector-certs

# Install the config map
kubectl apply -f ${istio}/install/kubernetes/istio-sidecar-injector-configmap-release.yaml

cat ${istio}/install/kubernetes/istio-sidecar-injector.yaml | \
     ${istio}/install/kubernetes/webhook-patch-ca-bundle.sh > \
     ${istio}/install/kubernetes/istio-sidecar-injector-with-ca-bundle.yaml

kubectl apply -f ${istio}/install/kubernetes/istio-sidecar-injector-with-ca-bundle.yaml

# Show istio deployment
kubectl -n istio-system get deployment -listio=sidecar-injector

# Label the default namespace with auto-sidecar-injection
kubectl label namespace default istio-injection=enabled || true