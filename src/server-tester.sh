#!/bin/bash

F=0
NUM_GROUPS=1
CONFIG="shard-r0.config"
PROTOCOL="indicus"
STORE=${PROTOCOL}store
DURATION=10
ZIPF=0.00
NUM_OPS_TX=10
NUM_KEYS_IN_DB=100000
KEY_PATH="/usr/local/etc/indicus-keys/donna"
BATCH_SIZE=64




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

<< COMMENTOUT

echo '[1] Shutting down possibly open servers'
for j in `seq 0 $((NUM_GROUPS-1))`; do
	for i in `seq 0 $((N-1))`; do
		#echo $((8000+$j*$N+$i))
		lsof -ti:$((8000+i)) | xargs kill -9 &>/dev/null   
	done;
done;
killall store/server


COMMENTOUT

echo '[2] Starting new servers'
echo 'NUM_GROUPS-1 : '$((NUM_GROUPS-1)) 
echo 'N-1 : ' $((N-1))
for j in `seq 0 $((NUM_GROUPS-1))`; do
	#echo 'Starting Group' $j
	for i in `seq 0 $((N-1))`; do
		 echo 'Starting Replica' $i
		 #DEBUG=store/indicusstore/*
		  store/server --config_path $CONFIG --group_idx $j \
		  --num_groups $NUM_GROUPS --num_shards $NUM_GROUPS --replica_idx $i --protocol $PROTOCOL \
			--num_keys $NUM_KEYS_IN_DB --zipf_coefficient $ZIPF --num_ops $NUM_OPS_TX --indicus_key_path $KEY_PATH &> server3.out
	done;
done;

#DEBUG=store/indicusstore/* store/server --config_path shard-r0.config --group_idx 0 --num_groups 1 --num_shards 1 --replica_idx 0 --protocol indicus --num_keys 10000000 --zipf_coefficient 0.9 --num_ops 10 --indicus_key_path /usr/local/etc/indicus-keys/donna &> server.out


#src/store/server.ccを実行している
#store
