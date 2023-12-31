#!/bin/bash

F=0
NUM_GROUPS=1
CONFIG="shard-r6.config"
PROTOCOL="indicus"
STORE=${PROTOCOL}store
DURATION=1
ZIPF=0.00
NUM_OPS_TX=10
NUM_KEYS_IN_DB=1000000
KEY_PATH="/usr/local/etc/indicus-keys/donna"
BATCH_SIZE=2
REPLICA_ID=2
GROUP_ID=0



while getopts f:g:cpath:p:d:z:num_ops:num_keys: option; do
case "${option}" in
f) F=${OPTARG};;
g) NUM_GROUPS=${OPTARG};;
cpath) CONFIG=${OPTARG};;
p) PROTOCOL=${OPTARG};;
d) DURATION=${OPTARG};;
z) ZIPF=${OPTARG};;
num_ops) NUM_OPS_TX=${OPTARG};;
num_keys) NUM_KEYS_IN_DB=${OPTARG};;
esac;
done

N=$((5*$F+1))

echo 'Starting Replica :'$((REPLICA_ID))
# DEBUG=store/indicusstore/*
store/server --config_path $CONFIG --group_idx $GROUP_ID \
 --num_groups $NUM_GROUPS --num_shards $NUM_GROUPS --replica_idx $REPLICA_ID --protocol $PROTOCOL \
 --num_keys $NUM_KEYS_IN_DB --zipf_coefficient $ZIPF --num_ops $NUM_OPS_TX --indicus_key_path $KEY_PATH &> server$((REPLICA_ID)).out


#src/store/server.cc?~B~R?~_?~L?~A~W?~A??~A~D?~B~K
#store
