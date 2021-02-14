#!/bin/bash

source env.exp

aws s3api create-bucket \
        --bucket $KUBE_BINARY_STORE_NAME \
        --region $AWS_REGION \
        --create-bucket-configuration LocationConstraint=$AWS_REGION

aws s3api put-bucket-versioning --bucket $KUBE_BINARY_STORE_NAME --versioning-configuration Status=Enabled
