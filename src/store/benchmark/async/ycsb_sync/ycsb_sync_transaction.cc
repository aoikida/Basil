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
#include "store/benchmark/async/ycsb_sync/ycsb_sync_transaction.h"
#include <cstdlib> 

namespace ycsb {

SyncYCSBTransaction::SyncYCSBTransaction(KeySelector *keySelector, int numOps, bool readOnly, int batchSize, int readRatio, int numKeys, std::mt19937 &rand)
 : keySelector(keySelector), numOps(numOps), readOnly(readOnly), batchSize(batchSize), readRatio(readRatio), numKeys(numKeys){
  for (int i = 0; i < numOps * batchSize; ++i) {
    uint64_t key = keySelector->GetKey(rand);
    keyIdxs.push_back(key);
  }
}

SyncYCSBTransaction::~SyncYCSBTransaction() {
}
/*
transaction_status_t SyncYCSBTransaction::Execute(SyncClient &client){

  client.Begin(timeout);


  
  //returnでtransaction_status_tを返す必要がある
}
*/

/*
std::vector<transaction_status_t> SyncYCSBTransaction::Execute_batch(SyncClient &client){

    client.Execute_batch(currTxn,
    std::bind(&AsyncTransactionBenchClient::ExecuteCallback_batch, this,
      std::placeholders::_1, std::placeholders::_2));
  
  //returnでstd::vector<transaction_status_t>を返す必要がある
}
*/

Operation SyncYCSBTransaction::GetNextOperation(size_t outstandingOpCount, size_t finishedOpCount,
    std::map<std::string, std::string> readValues) {
  if (outstandingOpCount < GetNumOps()) {
    //std::cerr << "outstanding: " << outstandingOpCount << "; finished: " << finishedOpCount << "num ops: " << GetNumOps() << std::endl;
    if(finishedOpCount != outstandingOpCount){
      return Wait();
    }
    else if (readOnly || outstandingOpCount % 2 == 0) {
      //std::cerr << "read: " << GetKey(finishedOpCount) << std::endl;
      return Get(GetKey(finishedOpCount));
    } else  {
      //std::cerr << "write: " << GetKey(finishedOpCount) << std::endl;
      auto strValueItr = readValues.find(GetKey(finishedOpCount));
      UW_ASSERT(strValueItr != readValues.end());
      std::string strValue = strValueItr->second;
      std::string writeValue;
      if (strValue.length() == 0) {
        writeValue = std::string(100, '\0'); //make a longer string
      } else {
        uint64_t intValue = 0;
        for (int i = 0; i < 100; ++i) {
          intValue = intValue | (static_cast<uint64_t>(strValue[i]) << ((99 - i) * 8));
        }
        intValue++;
        for (int i = 0; i < 100; ++i) {
          writeValue += static_cast<char>((intValue >> (99 - i) * 8) & 0xFF);
        }
      }
      return Put(GetKey(finishedOpCount), writeValue);
    }
  } else if (finishedOpCount == GetNumOps()) {
    return Commit();
  } else {
    return Wait();
  }
}


Operation SyncYCSBTransaction::GetNextOperation_batch(size_t outstandingOpCount, size_t finishedOpCount,
    std::map<std::string, std::string> readValues, int batchSize, Xoroshiro128Plus &rnd, FastZipf &zipf) {

  Debug("readRatio:%d\n", readRatio);
  Debug("numKeys:%d\n", numKeys);



  if (outstandingOpCount < GetNumOps() * batchSize) {
    Debug("outstanding: %d, finished : %d, num ops: %d, batchSize: %d", outstandingOpCount, finishedOpCount, GetNumOps(), batchSize);
    if(finishedOpCount != outstandingOpCount){
      return Wait();
    }
    else if ((rnd.next() % 100) < readRatio) {
      std::string key = keySelector->GetKey(zipf() % numKeys);
      Debug("read:%d\n", key);
      return Get(key);
    } 
    else  {
      std::string key = keySelector->GetKey(zipf() % numKeys);
      Debug("write:%d\n", key);
      std::string writeValue;
      writeValue = std::string(100, '\0'); //make a longer string
      return Put(key, writeValue);
    }
  } 
  else {
    std::cerr << "unnecessary transaction is made" << std::endl;
    exit(1);
  }
}

} // namespace ycsb
