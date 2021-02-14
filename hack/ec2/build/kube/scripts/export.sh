#!/bin/bash

source env.exp

kops get $NAME -o yaml > $NAME.yaml
