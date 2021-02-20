perfd
==

Tooling and setup for experiments in paper: [On the Use of ML for Blackbox System Performance Prediction](https://www.usenix.org/conference/nsdi21/presentation/fu). 

## Trial configuration

The applications - whose performance we want to study - are run on EC2 cloud (or any cloud providers that run [Kubernetes](https://kubernetes.io/)). We provide tools under `/apps` to for repeating the setup and experiments in our paper. To configure an application, e.g., memcached, simply modify the configuration files (or create new ones!) in `perfd/apps/memcached/manifests`. An example configuration for the memcached's varying key size experiment is as follows: 

```yaml
apiVersion: perfd/v1
kind: trialConfig
metadata:
  name: keySize_linear_1_10_suite_1
  appName: memcached
  appVersion: 1.4.33
  resultBucket: perfd
  logBucket: perfd-log
  autoscale: True
  clusterdown: True
  verbosity: 1
spec:
  instanceGroup:
    - image: 238764668013/perfd-generic
      tenancy: dedicated
      instanceType: c5.xlarge
      numInstance: 15
  metaConfig:
    numRun: 20
  resourceConfig:
    numServerInstance: 1
    numClientInstance: 1
    serverInstanceType: c5.xlarge
    clientInstanceType: c5.xlarge
  appConfig:
    keySize: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    valueSize: 200
    serverThread: 4
    clientThread: 1
    runTime: 10
    waitTime: 2
    warmupTime: 2
```

The `trial.py` implements a scheduler that will schedule the experiments on a cluster of machines and ensure that all trials  are run in parallel and in isolation (i.e., trials will use machines iff those machines do not have any running trial). 

Finally, to add a new application, simply add a run.py (see example under, e.g., `/apps/memcached/run.py`). 

## Models

ML models (including the probablistic random forests and MDNs) can be found under `/oracle/learn/`.

## Misc

With EC2 credentials configured, `make ec2 ` will create a k8s cluster on AWS EC2 cloud.

