SHELL := /bin/bash


# Extra environment variables
.EXPORT_ALL_VARIABLES:
OUT_DIR ?= _output
BIN_DIR := $(OUT_DIR)/bin

# Common targets
.PHONY: dep install clean

dep:
	go get k8s.io/klog

install: install-cli

clean:
	@rm -r $(OUT_DIR) || true


# Build and test kmc cli
.PHONY: cli install-cli

cli:
	go build -o $(BIN_DIR)/kmctl ./cmd/kmctl

install-cli: | cli
	cp $(BIN_DIR)/kmctl /usr/local/bin

# create test clusters
.PHONY: ec2, delete-ec2, unset

ec2:
	REGION=$(REGION); python3 -m hack.ec2.build.kube.cluster up

delete-ec2:
	python -m hack.ec2.build.kube.gen_spec $(CLUSTERID); python -m hack.ec2.build.kube.cluster down

unset:
	bash ./hack/unset-cluster.sh $(CLUSTERID)

# run applications
.PHONY: app

app:
	python -m apps.memcached.run $(APP)
