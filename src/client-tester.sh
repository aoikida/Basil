#!/bin/bash

CLIENTS=1 #クライアントプロセスの数 
CLIENT_THREAD=12 #クライアントの中のスレッドの数
F=0 #Fault nodeの数??
NUM_GROUPS=1 #シャードの数
CONFIG="shard-r6.config" #設定ファイルは"shard-r1.config"
PROTOCOL="indicus" #プロトコルは"indicus(Basilのこと)""
STORE=PROTOCOL+"store" #store = "indicusstore"
DURATION=30 #実行時間は10秒
ZIPF=0.0 #skew= 0
NUM_OPS_TX=10 #トランザクション内のオペレーション数
NUM_KEYS_IN_DB=100000 #データベース内のレコードの数
KEY_PATH="/usr/local/etc/indicus-keys/donna" #keyのパス
BENCHMARK="ycsb" #rwとretwis以外使用できない #smallbankとtpcc-syncは同期環境を想定しているので工夫が必要かも
BATCH_SIZE=1




#これらのオプションが実行時に呼ばれたら、変数に実行時に指定した引数が代入される。
#./client_tester.shでは、オプション及び引数をつけていないので、この一連のプログラムは呼び出されず、上記の値が使用されるはず。
while getopts c:f:g:cpath:p:d:z:num_ops:num_keys: option; do
case "${option}" in
c) CLIENTS=${OPTARG};;
f) F=${OPTARG};;
g) NUM_GROUPS=${OPTARG};;
cpath) CONFIG=${OPTARG};;
p) PROTOCOL=${OPTARG};;
d) DURATION=${OPTARG};;
z) ZIPF=${OPTARG};;
num_ops) NUM_OPS_TX=${OPTARG};;
num_keys) NUM_KEYS_IN_DB=${OPTARG};
esac;
done



N=$((5*$F+1))

echo '[1] Starting new clients'
for i in `seq 0 $((CLIENTS-1))`; do
  echo $i
  #valgrind
  #DEBUG=store/indicusstore/*
   store/benchmark/async/benchmark --config_path $CONFIG --num_groups $NUM_GROUPS \
    --num_shards $NUM_GROUPS \
    --protocol_mode $PROTOCOL --num_keys $NUM_KEYS_IN_DB --benchmark $BENCHMARK --num_ops $NUM_OPS_TX \
    --exp_duration $DURATION --client_id $i --num_clients $CLIENT_THREAD --warmup_secs 0 --cooldown_secs 0 \
    --key_selector zipf --zipf_coefficient $ZIPF --stats_file "stats-0.json" --indicus_key_path $KEY_PATH \
    --indicus_batch_size $BATCH_SIZE  &> client-9.out &
done;

sleep $((DURATION+3))
echo '[2] Shutting down possibly open servers and clients'
#killall store/benchmark/async/benchmark
#killall store/server

