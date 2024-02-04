/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Matthew Burke <matthelb@cs.cornell.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/
#include "store/common/frontend/async_adapter_client.h"

AsyncAdapterClient::AsyncAdapterClient(Client *client, uint32_t timeout) :
    client(client), timeout(timeout), outstandingOpCount(0UL), finishedOpCount(0UL) {
}

AsyncAdapterClient::~AsyncAdapterClient() {
}

void AsyncAdapterClient::Execute(AsyncTransaction *txn,
    execute_callback ecb, bool retry) {
  currEcb = ecb;
  currTxn = txn;
  outstandingOpCount = 0UL;
  finishedOpCount = 0UL;
  readValues.clear(); //readVaulesの初期化
  //indicusstoreのclient.ccのbeginに通じている。
  client->Begin([this](uint64_t id) {
    ExecuteNextOperation();
  }, []{}, timeout, retry);
  //ExecuteNextOperationで次のオペレーションを指定している。
}

void AsyncAdapterClient::Execute_ycsb(AsyncTransaction *txn,
    execute_callback ecb, bool retry) {
  currEcb = ecb;
  currTxn = txn;
  outstandingOpCount = 0UL;
  finishedOpCount = 0UL;
  readValues.clear(); //readVaulesの初期化
  //indicusstoreのclient.ccのbeginに通じている。
  client->Begin_ycsb([this](uint64_t id, Xoroshiro128Plus &rnd, FastZipf &zipf) {
    ExecuteNextOperation_ycsb(rnd, zipf);
  }, []{}, timeout, retry);
  //ExecuteNextOperationで次のオペレーションを指定している。
}

void AsyncAdapterClient::Execute_batch(AsyncTransaction *txn,
    execute_callback_batch ecb, bool retry) {
  currEcbb = ecb;
  currTxn = txn;
  readValues.clear();
  client->Begin_batch([this](uint64_t txNum, uint64_t txSize, uint64_t batchSize, Xoroshiro128Plus &rnd, FastZipf &zipf, std::vector<int> abort_tx_nums) {
    MakeTransaction_multi_abort(txNum, txSize, batchSize, rnd, zipf, abort_tx_nums);
  }, []{}, timeout, retry);
}

void AsyncAdapterClient::MakeTransaction_no_abort(uint64_t txNum, uint64_t txSize, uint64_t batchSize, Xoroshiro128Plus &rnd, FastZipf &zipf, std::vector<int> abort_tx_nums){

  int tx_num = 0;
  int thisTxWrite = 0;
  int outstandingOpCount_for_batch = 0UL;
  int finishedOpCount_for_batch = 0UL;

  //旋回のバッチで使用した変数を初期化
  transaction.clear();
  keyTxMap.clear(); //get_batchにおいて、keyからtxのidを割り出すために使用
  read_set.clear(); 
  readOpNum = 0;
  write_set.clear();
  writeOpNum = 0;
  commitTxNum = 0;
  
  //前回のバッチでabortになったトランザクションをバッチに含む。
  for(auto itr = abort_tx_nums.begin(); itr != abort_tx_nums.end(); ++itr){
    Debug("abort transaction in previous batch\n");
    Debug("abort_tx_nums_size : %d\n", abort_tx_nums.size());
    Debug("abort_tx_no, %d\n", *itr);
    std::vector<Operation> tx = batch.at(*itr);
    batch.erase(*itr);
    for (int op_num = 0; op_num < txSize; op_num++){
      Operation op = tx[op_num];
      switch (op.type) {
        case GET: {
          pre_read_set.push_back(op);
          break;
        }
        case PUT: {
          pre_write_set.push_back(op);
          break;
        }
      }
      outstandingOpCount_for_batch++;
      finishedOpCount_for_batch++;
    }
    
    Debug("%d : transaction finish\n", tx_num);
    for(auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
      readValues.insert(std::make_pair((*itr).key, ""));
      read_set.push_back(*itr);
      transaction.push_back(*itr);
      keyTxMap.insert(std::make_pair((*itr).key, tx_num));
      readOpNum++;
    }
    for(auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
      write_set.push_back(*itr);
      transaction.push_back(*itr);
      keyTxMap.insert(std::make_pair((*itr).key, tx_num));
      writeOpNum++;
      thisTxWrite++;
    }

    if (thisTxWrite != 0){
      ExecuteWriteOperation(tx_num, pre_write_set);
      thisTxWrite == 0;
    }

    pre_write_set.clear();
    pre_read_set.clear();
    batch.insert(std::make_pair(txNum + tx_num, transaction));
    transaction.clear();
    tx_num++;
    commitTxNum++;
  }

  //通常のトランザクションを生成する部分
  while(tx_num < batchSize){
    Debug("tx_num: %d\n", tx_num);
    for (int op_num = 0; op_num < txSize; op_num++){
      Operation op = currTxn->GetNextOperation_batch(outstandingOpCount_for_batch, finishedOpCount_for_batch,
          readValues, batchSize, rnd, zipf);
      switch (op.type) {
        case GET: {
          pre_read_set.push_back(op);
          break;
        }
        case PUT: {
          pre_write_set.push_back(op);
          break;
        }
      }
      outstandingOpCount_for_batch++;
      finishedOpCount_for_batch++;
    }

    Debug("%d : transaction finish\n", tx_num);
    for(auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
      readValues.insert(std::make_pair((*itr).key, ""));
      read_set.push_back(*itr);
      transaction.push_back(*itr);
      keyTxMap.insert(std::make_pair((*itr).key, tx_num));
      readOpNum++;
    }
    for(auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
      write_set.push_back(*itr);
      transaction.push_back(*itr);
      keyTxMap.insert(std::make_pair((*itr).key, tx_num));
      writeOpNum++;
      thisTxWrite++;
    }

    if (thisTxWrite != 0){
      ExecuteWriteOperation(tx_num, pre_write_set);
      thisTxWrite == 0;
    }

    pre_write_set.clear();
    pre_read_set.clear();
    batch.insert(std::make_pair(txNum + tx_num, transaction));
    transaction.clear();
    tx_num++;
    commitTxNum++;
  }

  if (writeOpNum == 0){
    ExecuteReadOperation();
  }
}

void AsyncAdapterClient::MakeTransaction_single_abort(uint64_t txNum, uint64_t txSize, uint64_t batchSize, Xoroshiro128Plus &rnd, FastZipf &zipf, std::vector<int> abort_tx_nums){

  int tx_num = 0;
  bool tx_conflict_finish = false;
  bool op_conflict_finish = false;
  int thisTxWrite = 0;
  int outstandingOpCount_for_batch = 0UL;
  int finishedOpCount_for_batch = 0UL;
  bool duplicate = false;
  

  //旋回のバッチで使用した変数を初期化
  transaction.clear();
  keyTxMap.clear(); //get_batchにおいて、keyからtxのidを割り出すために使用
  read_set.clear(); 
  readOpNum = 0;
  write_set.clear();
  writeOpNum = 0;
  commitTxNum = 0;
  txNum_writeSet.clear();
  
  
  //前回のバッチでconflictが発生し、バッチに含まれなかったトランザクションがある場合、バッチにそのトランザクションを含む。
  // pre_read_setとpre_write_setにロックをつけるか。
  if (pre_read_set.size() != 0 || pre_write_set.size() != 0){
    Debug("previous transaction remains\n");
    Debug("tx_num: %d\n", tx_num);
    for(auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
      read_set.push_back(*itr);
      transaction.push_back(*itr);
      keyTxMap.insert(std::make_pair((*itr).key, tx_num));
      readOpNum++;
    }
    for(auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
      write_set.push_back(*itr);
      transaction.push_back(*itr);
      conflict_write_set.push_back(*itr);
      keyTxMap.insert(std::make_pair((*itr).key, tx_num));
      writeOpNum++;
    }
    if (conflict_write_set.size() != 0){
        ExecuteWriteOperation(txNum, conflict_write_set);
        conflict_write_set.clear();
    }
    pre_write_set.clear();
    pre_read_set.clear();
    batch.insert(std::make_pair(txNum + tx_num, transaction));
    transaction.clear();
    outstandingOpCount_for_batch++;
    finishedOpCount_for_batch++;
    tx_num++;
    commitTxNum++;
  }
  
  //前回のバッチでabortになったトランザクションをバッチに含む。
  for(auto itr = abort_tx_nums.begin(); itr != abort_tx_nums.end(); ++itr){
    if (tx_num >= batchSize) break;
    Debug("abort transaction in previous batch\n");
    Debug("abort_tx_nums_size : %d\n", abort_tx_nums.size());
    Debug("abort_tx_no, %d\n", *itr);
    std::vector<Operation> tx = batch.at(*itr);
    batch.erase(*itr);
    for (int op_num = 0; op_num < txSize; op_num++){
      Operation op = tx[op_num];
      switch (op.type) {
        case GET: {
          pre_read_set.push_back(op);
          for (auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
            if ((*itr).key == op.key){
              duplicate = true;
              break;
            }
          }
          if (duplicate == true){
            duplicate = false;
            continue;
          }
          //このwriteによって、同一バッチ内でconflictが発生するか否かを検証。
          //発生する場合、このreadを含むトランザクションは次回のバッチに回し、このトランザクションを除いたバッチを作成する。
          for(auto itr = write_set.begin(); itr != write_set.end(); ++itr){
            if ((*itr).key == op.key){
              //バッチをこのトランザクションを除いて作成する
              Debug("conflict occur");
              if (tx_conflict_finish == false){
                commitTxNum = tx_num;
              }
              tx_conflict_finish = true;
              tx_num = batchSize;
            }
            break;
          }
          for (auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
            if ((*itr).key == op.key){
              Debug("conflict occur in same transaction");
              if (readwrite == true){
                if (tx_conflict_finish == false){
                  commitTxNum = tx_num;
                }
                op_conflict_finish = true;
                tx_num = batchSize;
              }
              else {
                writeread = true;
              }
            }
            break;
          }
          break;
        }
        case PUT: {
          pre_write_set.push_back(op);
          for (auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
            if ((*itr).key == op.key){
              duplicate = true;
              break;
            }
          }
          if (duplicate == true){
            duplicate = false;
            continue;
          }
          for(auto itr = read_set.begin(); itr != read_set.end(); ++itr){
            if ((*itr).key == op.key){
              if (tx_conflict_finish == false){
                commitTxNum = tx_num;
              }
              tx_conflict_finish = true;
              tx_num = batchSize;
            }
            break;
          }
          for (auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
            if ((*itr).key == op.key){
              Debug("conflict occur in same transaction");
              if (writeread == true){
                if (tx_conflict_finish == false){
                  commitTxNum = tx_num;
                }
                op_conflict_finish = true;
                tx_num = batchSize;
              }
              else {
                readwrite = true;
              }
            }
            break;
          }
          break;
        }
      }
      outstandingOpCount_for_batch++;
      finishedOpCount_for_batch++;
    }
    if (tx_conflict_finish == false && op_conflict_finish == false){
      Debug("%d : transaction finish\n", tx_num);
      for(auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
        readValues.insert(std::make_pair((*itr).key, ""));
        read_set.push_back(*itr);
        transaction.push_back(*itr);
        keyTxMap.insert(std::make_pair((*itr).key, tx_num));
        readOpNum++;
      }
      for(auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
        write_set.push_back(*itr);
        transaction.push_back(*itr);
        keyTxMap.insert(std::make_pair((*itr).key, tx_num));
        writeOpNum++;
        thisTxWrite++;
      }

      /*
      if (thisTxWrite != 0){
        ExecuteWriteOperation(tx_num, pre_write_set);
        thisTxWrite == 0;
      }
      */

      txNum_writeSet.push_back(std::make_pair(tx_num, pre_write_set));

      pre_write_set.clear();
      pre_read_set.clear();
      batch.insert(std::make_pair(txNum + tx_num, transaction));
      transaction.clear();
      tx_num++;
      commitTxNum++;
    }
  }

  if (op_conflict_finish) goto MAKE_TX_FIN;
  if (tx_conflict_finish) goto MAKE_TX_FIN;


  //通常のトランザクションを生成する部分
  while(tx_num < batchSize){
    Debug("tx_num: %d\n", tx_num);
    for (int op_num = 0; op_num < txSize; op_num++){
      Operation op = currTxn->GetNextOperation_batch(outstandingOpCount_for_batch, finishedOpCount_for_batch,
          readValues, batchSize, rnd, zipf);
      switch (op.type) {
        case GET: {
          pre_read_set.push_back(op);
          for (auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
            if ((*itr).key == op.key){
              duplicate = true;
              break;
            }
          }
          if (duplicate == true){
            duplicate = false;
            continue;
          }
          //このwriteによって、同一バッチ内でconflictが発生するか否かを検証。
          //発生する場合、このreadを含むトランザクションは次回のバッチに回し、このトランザクションを除いたバッチを作成する。
          for(auto itr = write_set.begin(); itr != write_set.end(); ++itr){
            if ((*itr).key == op.key){
              //バッチをこのトランザクションを除いて作成する
              Debug("conflict occur");
              if (tx_conflict_finish == false){
                commitTxNum = tx_num;
              }
              Debug("commitTxNum: %d\n", commitTxNum);
              tx_conflict_finish = true;
              tx_num = batchSize;
            }
            break;
          }
          for (auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
            if ((*itr).key == op.key){
              Debug("conflict occur in same transaction");
              if (readwrite == true){
                if (tx_conflict_finish == false){
                  commitTxNum = tx_num;
                }
                op_conflict_finish = true;
                tx_num = batchSize;
              }
              else {
                writeread = true;
              }
            }
            break;
          }
          break;
        }
        case PUT: {
          pre_write_set.push_back(op);
          for (auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
            if ((*itr).key == op.key){
              duplicate = true;
              break;
            }
          }
          if (duplicate == true){
            duplicate = false;
            continue;
          }
          for(auto itr = read_set.begin(); itr != read_set.end(); ++itr){
            if ((*itr).key == op.key){
              Debug("conflict occur");
              if (tx_conflict_finish == false){
                commitTxNum = tx_num;
              }
              tx_conflict_finish = true;
              tx_num = batchSize;
            }
            break;
          }
          for (auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
            if ((*itr).key == op.key){
              Debug("conflict occur in same transaction");
              if (writeread == true){
                if (tx_conflict_finish == false){
                  commitTxNum = tx_num;
                }
                op_conflict_finish = true;
                tx_num = batchSize;
              }
              else {
                readwrite = true;
              }
            }
            break;
          }
          break;
        }
      }
      outstandingOpCount_for_batch++;
      finishedOpCount_for_batch++;
    }
    if (tx_conflict_finish == false && op_conflict_finish == false){
      Debug("%d : transaction finish\n", tx_num);
      for(auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
        readValues.insert(std::make_pair((*itr).key, ""));
        read_set.push_back(*itr);
        transaction.push_back(*itr);
        keyTxMap.insert(std::make_pair((*itr).key, tx_num));
        readOpNum++;
      }
      for(auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
        write_set.push_back(*itr);
        transaction.push_back(*itr);
        keyTxMap.insert(std::make_pair((*itr).key, tx_num));
        writeOpNum++;
        thisTxWrite++;
      }

      /*
      if (thisTxWrite != 0){
        ExecuteWriteOperation(tx_num, pre_write_set);
        thisTxWrite == 0;
      }
      */

      txNum_writeSet.push_back(std::make_pair(tx_num, pre_write_set));

      pre_write_set.clear();
      pre_read_set.clear();
      batch.insert(std::make_pair(txNum + tx_num, transaction));
      transaction.clear();
      tx_num++;
      commitTxNum++;
    }
  }

MAKE_TX_FIN:

  if (readwrite == true || writeOpNum == 0){
    //readを先に行う
    ExecuteReadOperation();
  }
  else{
    //writeを先に行う // 通常
    for (auto itr = txNum_writeSet.begin(); itr != txNum_writeSet.begin(); ++itr){
      for (int i = 0; i < (itr->second).size(); i++){
        ExecuteWriteOperation(itr->first, itr->second);
      }
    }
  }

  if (op_conflict_finish == true){
    if (readwrite == true){
      readwrite = false;
      writeread = true;
    }
  }
  else{
    readwrite = false;
    writeread = false;
  }
}

void AsyncAdapterClient::MakeTransaction_multi_abort(uint64_t txNum, uint64_t txSize, uint64_t batchSize, Xoroshiro128Plus &rnd, FastZipf &zipf, std::vector<int> abort_tx_nums){

  int tx_num = 0;
  bool tx_conflict_finish = false;
  bool op_conflict_finish = false;
  int thisTxWrite = 0;
  int outstandingOpCount_for_batch = 0UL;
  int finishedOpCount_for_batch = 0UL;
  bool duplicate = false;
  

  //旋回のバッチで使用した変数を初期化
  transaction.clear();
  keyTxMap.clear(); //get_batchにおいて、keyからtxのidを割り出すために使用
  read_set.clear(); 
  readOpNum = 0;
  write_set.clear();
  writeOpNum = 0;
  commitTxNum = 0;
  txNum_writeSet.clear();
  
  
  for (auto tx = abort_set.begin(); tx != abort_set.end(); ++tx){
    if (tx_num >= batchSize) break;
    for (int op_num = 0; op_num < txSize; op_num++){
      Operation op = (*tx)[op_num];
      switch (op.type) {
        case GET: {
          pre_read_set.push_back(op);
          for (auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
            if ((*itr).key == op.key){
              duplicate = true;
              break;
            }
          }
          if (duplicate == true){
            duplicate = false;
            continue;
          }
          for(auto itr = write_set.begin(); itr != write_set.end(); ++itr){
            if ((*itr).key == op.key){
              //バッチをこのトランザクションを除いて作成する
              Debug("conflict occur");
              tx_conflict_finish = true;
            }
            break;
          }
          for (auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
            if ((*itr).key == op.key){
              Debug("conflict occur in same transaction");
              if (readwrite == true){
                op_conflict_finish = true;
              }
              else {
                writeread = true;
              }
            }
            break;
          }
          break;
        }
        case PUT: {
          pre_write_set.push_back(op);
          for (auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
            if ((*itr).key == op.key){
              duplicate = true;
              break;
            }
          }
          if (duplicate == true){
            duplicate = false;
            continue;
          }
          for(auto itr = read_set.begin(); itr != read_set.end(); ++itr){
            if ((*itr).key == op.key){
              tx_conflict_finish = true;
            }
            break;
          }
          for (auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
            if ((*itr).key == op.key){
              Debug("conflict occur in same transaction");
              if (writeread == true){
                op_conflict_finish = true;
              }
              else {
                readwrite = true;
              }
            }
            break;
          }
          break;
        }
      }
      outstandingOpCount_for_batch++;
      finishedOpCount_for_batch++;
    }
    if (tx_conflict_finish == false && op_conflict_finish == false){
      Debug("%d : transaction finish\n", tx_num);
      for(auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
        readValues.insert(std::make_pair((*itr).key, ""));
        read_set.push_back(*itr);
        transaction.push_back(*itr);
        keyTxMap.insert(std::make_pair((*itr).key, tx_num));
        readOpNum++;
      }
      for(auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
        write_set.push_back(*itr);
        transaction.push_back(*itr);
        keyTxMap.insert(std::make_pair((*itr).key, tx_num));
        writeOpNum++;
        thisTxWrite++;
      }

      if (thisTxWrite != 0){
        ExecuteWriteOperation(tx_num, pre_write_set);
        thisTxWrite == 0;
      }

      pre_write_set.clear();
      pre_read_set.clear();
      batch.insert(std::make_pair(txNum + tx_num, transaction));
      transaction.clear();
      tx_num++;
      commitTxNum++;
      abort_set.erase(tx);
    }
    else {
      pre_write_set.clear();
      pre_read_set.clear();
      tx_conflict_finish = false;
      op_conflict_finish = false;
    }
  }
  
  //前回のバッチでabortになったトランザクションをバッチに含む。
  for(auto itr = abort_tx_nums.begin(); itr != abort_tx_nums.end(); ++itr){
    if (tx_num >= batchSize) break;
    Debug("abort transaction in previous batch\n");
    Debug("abort_tx_nums_size : %d\n", abort_tx_nums.size());
    Debug("abort_tx_no, %d\n", *itr);
    std::vector<Operation> tx = batch.at(*itr);
    batch.erase(*itr);
    for (int op_num = 0; op_num < txSize; op_num++){
      Operation op = tx[op_num];
      switch (op.type) {
          case GET: {
            pre_read_set.push_back(op);
            for (auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
              if ((*itr).key == op.key){
                duplicate = true;
                break;
              }
            }
            if (duplicate == true){
              duplicate = false;
              continue;
            }
            for(auto itr = write_set.begin(); itr != write_set.end(); ++itr){
              if ((*itr).key == op.key){
                //バッチをこのトランザクションを除いて作成する
                Debug("conflict occur");
                tx_conflict_finish = true;
              }
              break;
            }
            for (auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
              if ((*itr).key == op.key){
                Debug("conflict occur in same transaction");
                if (readwrite == true){
                  op_conflict_finish = true;
                }
                else {
                  writeread = true;
                }
              }
              break;
            }
            break;
          }
          case PUT: {
            pre_write_set.push_back(op);
            for (auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
              if ((*itr).key == op.key){
                duplicate = true;
                break;
              }
            }
            if (duplicate == true){
              duplicate = false;
              continue;
            }
            for(auto itr = read_set.begin(); itr != read_set.end(); ++itr){
              if ((*itr).key == op.key){
                tx_conflict_finish = true;
              }
              break;
            }
            for (auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
              if ((*itr).key == op.key){
                Debug("conflict occur in same transaction");
                if (writeread == true){
                  op_conflict_finish = true;
                }
                else {
                  readwrite = true;
                }
              }
              break;
            }
            break;
          }
        }
      outstandingOpCount_for_batch++;
      finishedOpCount_for_batch++;
    }
    if (tx_conflict_finish == false && op_conflict_finish == false){
      Debug("%d : transaction finish\n", tx_num);
      for(auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
        readValues.insert(std::make_pair((*itr).key, ""));
        read_set.push_back(*itr);
        transaction.push_back(*itr);
        keyTxMap.insert(std::make_pair((*itr).key, tx_num));
        readOpNum++;
      }
      for(auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
        write_set.push_back(*itr);
        transaction.push_back(*itr);
        keyTxMap.insert(std::make_pair((*itr).key, tx_num));
        writeOpNum++;
        thisTxWrite++;
      }

      /*
      if (thisTxWrite != 0){
        ExecuteWriteOperation(tx_num, pre_write_set);
        thisTxWrite == 0;
      }
      */

      txNum_writeSet.push_back(std::make_pair(tx_num, pre_write_set));

      pre_write_set.clear();
      pre_read_set.clear();
      batch.insert(std::make_pair(txNum + tx_num, transaction));
      transaction.clear();
      tx_num++;
      commitTxNum++;
    }
    else {
      for(auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
        transaction.push_back(*itr);
      }
      for(auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
        transaction.push_back(*itr);
      }
      abort_set.push_back(transaction);
      pre_write_set.clear();
      pre_read_set.clear();
      transaction.clear();
      tx_conflict_finish = false;
      op_conflict_finish = false;
    }
  }

  //通常のトランザクションを生成する部分
  while(tx_num < batchSize){
    Debug("tx_num: %d\n", tx_num);
    for (int op_num = 0; op_num < txSize; op_num++){
      Operation op = currTxn->GetNextOperation_batch(outstandingOpCount_for_batch, finishedOpCount_for_batch,
          readValues, batchSize, rnd, zipf);
      switch (op.type) {
        case GET: {
          pre_read_set.push_back(op);
          for (auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
            if ((*itr).key == op.key){
              duplicate = true;
              break;
            }
          }
          if (duplicate == true){
            duplicate = false;
            continue;
          }
          for(auto itr = write_set.begin(); itr != write_set.end(); ++itr){
            if ((*itr).key == op.key){
              //バッチをこのトランザクションを除いて作成する
              Debug("conflict occur");
              tx_conflict_finish = true;
            }
            break;
          }
          for (auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
            if ((*itr).key == op.key){
              Debug("conflict occur in same transaction");
              if (readwrite == true){
                op_conflict_finish = true;
              }
              else {
                writeread = true;
              }
            }
            break;
          }
          break;
        }
        case PUT: {
          pre_write_set.push_back(op);
          for (auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
            if ((*itr).key == op.key){
              duplicate = true;
              break;
            }
          }
          if (duplicate == true){
            duplicate = false;
            continue;
          }
          for(auto itr = read_set.begin(); itr != read_set.end(); ++itr){
            if ((*itr).key == op.key){
              tx_conflict_finish = true;
            }
            break;
          }
          for (auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
            if ((*itr).key == op.key){
              Debug("conflict occur in same transaction");
              if (writeread == true){
                op_conflict_finish = true;
              }
              else {
                readwrite = true;
              }
            }
            break;
          }
          break;
        }
      }
      outstandingOpCount_for_batch++;
      finishedOpCount_for_batch++;
    }
    if (tx_conflict_finish == false && op_conflict_finish == false){
      Debug("%d : transaction finish\n", tx_num);
      for(auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
        readValues.insert(std::make_pair((*itr).key, ""));
        read_set.push_back(*itr);
        transaction.push_back(*itr);
        keyTxMap.insert(std::make_pair((*itr).key, tx_num));
        readOpNum++;
      }
      for(auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
        write_set.push_back(*itr);
        transaction.push_back(*itr);
        keyTxMap.insert(std::make_pair((*itr).key, tx_num));
        writeOpNum++;
        thisTxWrite++;
      }

      /*
      if (thisTxWrite != 0){
        ExecuteWriteOperation(tx_num, pre_write_set);
        thisTxWrite == 0;
      }
      */

      txNum_writeSet.push_back(std::make_pair(tx_num, pre_write_set));

      pre_write_set.clear();
      pre_read_set.clear();
      batch.insert(std::make_pair(txNum + tx_num, transaction));
      transaction.clear();
      tx_num++;
      commitTxNum++;
    }
    else {
      for(auto itr = pre_read_set.begin(); itr != pre_read_set.end(); ++itr){
        transaction.push_back(*itr);
      }
      for(auto itr = pre_write_set.begin(); itr != pre_write_set.end(); ++itr){
        transaction.push_back(*itr);
      }
      abort_set.push_back(transaction);
      pre_write_set.clear();
      pre_read_set.clear();
      transaction.clear();
      tx_conflict_finish = false;
      op_conflict_finish = false;
    }
  }

  if (writeOpNum == 0){
    ExecuteReadOperation();
  }
  else if (readwrite == true){
    //readを先に行う
    ExecuteReadOperation();
  }
  else{
    //writeを先に行う // 通常
    writeread = true;
    for (auto itr = txNum_writeSet.begin(); itr != txNum_writeSet.end(); ++itr){
      ExecuteWriteOperation(itr->first, itr->second);
    }
  }

}


void AsyncAdapterClient::ExecuteWriteOperation(int tx_num, std::vector<Operation> write_set){

  for(auto itr = write_set.begin(); itr != write_set.end(); ++itr) {
    client->Put_batch((*itr).key, (*itr).value, std::bind(&AsyncAdapterClient::PutCallback_batch,
            this, std::placeholders::_1, std::placeholders::_2,
            std::placeholders::_3), std::bind(&AsyncAdapterClient::PutTimeout,
              this, std::placeholders::_1, std::placeholders::_2,
              std::placeholders::_3), tx_num, timeout);
  }

}



void AsyncAdapterClient::ExecuteReadOperation(){
  
  key_list.clear();

  for(auto itr = read_set.begin(); itr != read_set.end(); ++itr){
    gcb_list.push_back(std::bind(&AsyncAdapterClient::GetCallback_batch, this,
          std::placeholders::_1, std::placeholders::_2, std::placeholders::_3,
          std::placeholders::_4));
    key_list.push_back((*itr).key);
  }

  client->Get_batch(key_list, gcb_list, &keyTxMap, std::bind(&AsyncAdapterClient::GetTimeout_batch, this,
          std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4), timeout);
}


void AsyncAdapterClient::ExecuteCommit(){

  Debug("commitTxNum: %d\n", commitTxNum);

  client->Commit_batch(std::bind(&AsyncAdapterClient::CommitCallback_batch, this,
        std::placeholders::_1, std::placeholders::_2), std::bind(&AsyncAdapterClient::CommitTimeout,
          this), timeout, commitTxNum);

}

void AsyncAdapterClient::ExecuteNextOperation() {
  Debug("AsyncAdapterClient::ExecuteNextOperation");
  //GetNextOperationはstore/benchmark/async/rw/rw_transaction.ccのGetNextOperationである。
  Operation op = currTxn->GetNextOperation(outstandingOpCount, finishedOpCount,
      readValues);
  switch (op.type) {
    case GET: {
      client->Get(op.key, std::bind(&AsyncAdapterClient::GetCallback, this,
        std::placeholders::_1, std::placeholders::_2, std::placeholders::_3,
        std::placeholders::_4), std::bind(&AsyncAdapterClient::GetTimeout, this,
          std::placeholders::_1, std::placeholders::_2), timeout);
      ++outstandingOpCount;
      // timeout doesn't really matter?
      ExecuteNextOperation();
      break;
    }
    case PUT: {
      client->Put(op.key, op.value, std::bind(&AsyncAdapterClient::PutCallback,
            this, std::placeholders::_1, std::placeholders::_2,
            std::placeholders::_3), std::bind(&AsyncAdapterClient::PutTimeout,
              this, std::placeholders::_1, std::placeholders::_2,
              std::placeholders::_3), timeout);
      ++outstandingOpCount;
      // timeout doesn't really matter?
      ExecuteNextOperation();
      break;
    }
    case COMMIT: {
      client->Commit(std::bind(&AsyncAdapterClient::CommitCallback, this,
        std::placeholders::_1), std::bind(&AsyncAdapterClient::CommitTimeout,
          this), timeout);
      // timeout doesn't really matter?
      break;
    }
    case ABORT: {
      client->Abort(std::bind(&AsyncAdapterClient::AbortCallback, this),
          std::bind(&AsyncAdapterClient::AbortTimeout, this), timeout);
      // timeout doesn't really matter?
      currEcb(ABORTED_USER, std::map<std::string, std::string>());
      break;
    }
    case WAIT: {
      break;
    }
    default:
      NOT_REACHABLE();
  }
}

void AsyncAdapterClient::ExecuteNextOperation_ycsb(Xoroshiro128Plus &rnd, FastZipf &zipf) {
  Debug("AsyncAdapterClient::ExecuteNextOperation");
  //GetNextOperationはstore/benchmark/async/rw/rw_transaction.ccのGetNextOperationである。
  Operation op = currTxn->GetNextOperation_ycsb(outstandingOpCount, finishedOpCount,
      readValues, rnd, zipf);
  switch (op.type) {
    case GET: {
      client->Get_ycsb(op.key, std::bind(&AsyncAdapterClient::GetCallback_ycsb, this,
        std::placeholders::_1, std::placeholders::_2, std::placeholders::_3,
        std::placeholders::_4, std::placeholders::_5, std::placeholders::_6), std::bind(&AsyncAdapterClient::GetTimeout, this,
          std::placeholders::_1, std::placeholders::_2), timeout);
      ++outstandingOpCount;
      // timeout doesn't really matter?
      ExecuteNextOperation_ycsb(rnd, zipf);
      break;
    }
    case PUT: {
      client->Put_ycsb(op.key, op.value, std::bind(&AsyncAdapterClient::PutCallback_ycsb,
            this, std::placeholders::_1, std::placeholders::_2,
            std::placeholders::_3, std::placeholders::_4, std::placeholders::_5), std::bind(&AsyncAdapterClient::PutTimeout,
              this, std::placeholders::_1, std::placeholders::_2,
              std::placeholders::_3), timeout);
      ++outstandingOpCount;
      // timeout doesn't really matter?
      ExecuteNextOperation_ycsb(rnd, zipf);
      break;
    }
    case COMMIT: {
      client->Commit(std::bind(&AsyncAdapterClient::CommitCallback, this,
        std::placeholders::_1), std::bind(&AsyncAdapterClient::CommitTimeout,
          this), timeout);
      // timeout doesn't really matter?
      break;
    }
    case ABORT: {
      client->Abort(std::bind(&AsyncAdapterClient::AbortCallback, this),
          std::bind(&AsyncAdapterClient::AbortTimeout, this), timeout);
      // timeout doesn't really matter?
      currEcb(ABORTED_USER, std::map<std::string, std::string>());
      break;
    }
    case WAIT: {
      break;
    }
    default:
      NOT_REACHABLE();
  }
}

void AsyncAdapterClient::GetCallback(int status, const std::string &key,
    const std::string &val, Timestamp ts) {
  Debug("Get(%s) callback.", key.c_str());
  readValues.insert(std::make_pair(key, val));
  finishedOpCount++;
  ExecuteNextOperation();
}

void AsyncAdapterClient::GetCallback_ycsb(int status, const std::string &key,
    const std::string &val, Timestamp ts, Xoroshiro128Plus &rnd, FastZipf &zipf) {
  Debug("Get(%s) callback.", key.c_str());
  readValues.insert(std::make_pair(key, val));
  finishedOpCount++;
  ExecuteNextOperation_ycsb(rnd, zipf);
}


void AsyncAdapterClient::GetCallback_batch(int status, const std::string &key,
    const std::string &val, Timestamp ts) {
  Debug("Get(%s) callback.", key.c_str());
  readValues.insert(std::make_pair(key, val));
  getCbCount++;
  if (readOpNum <= getCbCount){
      if (writeOpNum != 0 && readwrite){
        for (auto itr = txNum_writeSet.begin(); itr != txNum_writeSet.begin(); ++itr){
          for (int i = 0; i < (itr->second).size(); i++){
            ExecuteWriteOperation(itr->first, itr->second);
          }
        }
      }
      else{
        ExecuteCommit();
      }
      getCbCount = 0;
  }
}

void AsyncAdapterClient::GetTimeout(int status, const std::string &key) {
  Warning("Get(%s) timed out :(", key.c_str());
  client->Get(key, std::bind(&AsyncAdapterClient::GetCallback, this,
        std::placeholders::_1, std::placeholders::_2, std::placeholders::_3,
        std::placeholders::_4), std::bind(&AsyncAdapterClient::GetTimeout, this,
          std::placeholders::_1, std::placeholders::_2), timeout);
}

void AsyncAdapterClient::GetTimeout_ycsb(int status, const std::string &key) {
  Warning("Get(%s) timed out :(", key.c_str());
  client->Get_ycsb(key, std::bind(&AsyncAdapterClient::GetCallback_ycsb, this,
        std::placeholders::_1, std::placeholders::_2, std::placeholders::_3,
        std::placeholders::_4, std::placeholders::_5, std::placeholders::_6), std::bind(&AsyncAdapterClient::GetTimeout, this,
          std::placeholders::_1, std::placeholders::_2), timeout);
}

void AsyncAdapterClient::GetTimeout_batch(int status, std::vector<std::string> key_list, std::vector<get_callback> gcb_list, uint32_t timeout) {
  Warning("Get_batch timed out");
  client->Get_batch(key_list, gcb_list, &keyTxMap, std::bind(&AsyncAdapterClient::GetTimeout_batch, this,
          std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4), timeout);
}

void AsyncAdapterClient::PutCallback(int status, const std::string &key,
    const std::string &val) {
  Debug("Put(%s,%s) callback.", key.c_str(), val.c_str());
  finishedOpCount++;
  ExecuteNextOperation();
}

void AsyncAdapterClient::PutCallback_ycsb(int status, const std::string &key,
    const std::string &val, Xoroshiro128Plus &rnd, FastZipf &zipf) {
  Debug("Put(%s,%s) callback.", key.c_str(), val.c_str());
  finishedOpCount++;
  ExecuteNextOperation_ycsb(rnd, zipf);
}

void AsyncAdapterClient::PutCallback_batch(int status, const std::string &key,
    const std::string &val){
    Debug("Put(%s,%s) callback.", key.c_str(), val.c_str());
    putCbCount++;
    if (writeOpNum <= putCbCount){
      if (readOpNum != 0 && writeread){
        ExecuteReadOperation();
      }
      else {
        ExecuteCommit();
      }
      putCbCount = 0;
    }
}

void AsyncAdapterClient::PutTimeout(int status, const std::string &key,
    const std::string &val) {
  Warning("Put(%s,%s) timed out :(", key.c_str(), val.c_str());
}


void AsyncAdapterClient::CommitCallback(transaction_status_t result) {
  Debug("Commit callback.");
  currEcb(result, readValues);
}

void AsyncAdapterClient::CommitCallback_batch(transaction_status_t result, int txId) {
  Debug("Commit callback_batch \n");
  commitCbCount++;
  if (result == COMMITTED){
    batch.erase(txId);
  }
  results.push_back(result);
  if (commitTxNum <= commitCbCount){
      currEcbb(results, readValues);
      commitCbCount = 0;
      results.clear();
  }
}

void AsyncAdapterClient::CommitTimeout() {
  Warning("Commit timed out :(");
}

void AsyncAdapterClient::AbortCallback() {
  Debug("Abort callback.");
}

void AsyncAdapterClient::AbortTimeout() {
  Warning("Abort timed out :(");
}
