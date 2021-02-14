#!/bin/bash

source env.exp

# default availability zone is zone 'a'

kops delete cluster --name $NAME --yes


