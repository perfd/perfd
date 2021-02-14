#!/bin/bash

source env.exp

aws s3api create-bucket \
        --bucket $KOPS_STATE_STORE_NAME \
        --region $AWS_REGION \
        --create-bucket-configuration LocationConstraint=$AWS_REGION

aws s3api put-bucket-versioning --bucket $KOPS_STATE_STORE_NAME --versioning-configuration Status=Enabled
