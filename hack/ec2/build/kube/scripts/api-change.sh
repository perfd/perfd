#!/usr/bin/env bash

source env.exp

cd ${KUBE_ROOT}

make clean && make generated_files
./hack/update-generated-protobuf.sh


