#!/usr/bin/env bash

aws ec2 describe-instances --query 'Reservations[*].Instances[*].[InstanceId,Tags[?Key==`Name`].Value|[0],State.Name,PrivateIpAddress,PublicIpAddress]' --output text | column -t | grep masters | awk '{print $2" "$5}' | cut -d'.' -f2- | cut -d'.' -f2-

