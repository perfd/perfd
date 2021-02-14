#!/usr/bin/env bash

source env.exp

# make kube binaries available on S3
# update KUBE_ROOT and KUBE_VERSION in env.exp if needed

s3-binary-store.sh || true
s3-state-store.sh || true

cd ${KUBE_ROOT}
#make quick-release

cd ./_output/release-tars/
tar zxf kubernetes-server-linux-amd64.tar.gz

rm kubernetes/server/bin/federation*
rm kubernetes/server/bin/hyperkube
rm kubernetes/server/bin/kubeadm
rm kubernetes/server/bin/kube-apiserver
rm kubernetes/server/bin/kube-controller-manager
rm kubernetes/server/bin/kube-discovery
rm kubernetes/server/bin/kube-dns
rm kubernetes/server/bin/kubemark
rm kubernetes/server/bin/kube-proxy
rm kubernetes/server/bin/kube-scheduler
rm kubernetes/kubernetes-src.tar.gz

find kubernetes/server/bin -type f -name "*.tar" | xargs -I {} /bin/bash -c "sha1sum {} | cut -f1 -d ' ' > {}.sha1"
find kubernetes/server/bin -type f -name "kube???" | xargs -I {} /bin/bash -c "sha1sum {} | cut -f1 -d ' ' > {}.sha1"

aws s3 sync --acl public-read kubernetes/server/bin/ s3://${KUBE_BINARY_STORE_NAME}/kubernetes/dev/${KUBE_VERSION}/bin/linux/amd64/
