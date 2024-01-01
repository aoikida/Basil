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
#include "store/benchmark/async/ycsb/ycsb_transaction.h"
#include <cstdlib> 

namespace ycsb {

YCSBTransaction::YCSBTransaction(KeySelector *keySelector, int numOps, bool readOnly, int batchSize, int readRatio, int numKeys, 
std::mt19937 &rand) : keySelector(keySelector), numOps(numOps), readOnly(readOnly), batchSize(batchSize), readRatio(readRatio), numKeys(numKeys){
  for (int i = 0; i < numOps * batchSize; ++i) {
    uint64_t key = keySelector->GetKey(rand);
    keyIdxs.push_back(key);
  }
}

YCSBTransaction::~YCSBTransaction() {
}

Operation YCSBTransaction::GetNextOperation(size_t outstandingOpCount, size_t finishedOpCount,
    std::map<std::string, std::string> readValues) {}

Operation YCSBTransaction::GetNextOperation_ycsb(size_t outstandingOpCount, size_t finishedOpCount,
    std::map<std::string, std::string> readValues, Xoroshiro128Plus &rnd, FastZipf &zipf) {
  if (outstandingOpCount < GetNumOps()) {
    //std::cerr << "outstanding: " << outstandingOpCount << "; finished: " << finishedOpCount << "num ops: " << GetNumOps() << std::endl;
    if(finishedOpCount != outstandingOpCount){
      return Wait();
    }
    else if ((rnd.next() % 100) < readRatio) {
      std::string key = keySelector->GetKey(zipf() % numKeys);
      return Get(key);
    } 
    else  {
      std::string key = keySelector->GetKey(zipf() % numKeys);
      std::string writeValue;
      writeValue = std::string(100, '\0'); //make a longer string
      return Put(key, writeValue);
    }
  } else if (finishedOpCount == GetNumOps()) {
    return Commit();
  } else {
    return Wait();
  }
}

Operation YCSBTransaction::GetNextOperation_batch(size_t outstandingOpCount, size_t finishedOpCount,
    std::map<std::string, std::string> readValues, int batchSize, Xoroshiro128Plus &rnd, FastZipf &zipf) {

  if (outstandingOpCount < GetNumOps() * batchSize) {
    Debug("outstanding: %d, finished: %d, num ops: %d, batchSize: %d \n", outstandingOpCount, finishedOpCount, GetNumOps(), batchSize);
    if ((rnd.next() % 100) < readRatio) {
      std::string key = keySelector->GetKey(zipf() % numKeys);
      Debug("read: %d\n", key);
      return Get(key);
    } 
    else  {
      std::string key = keySelector->GetKey(zipf() % numKeys);
      Debug("write: %d\n", key);
      std::string writeValue;
      writeValue = std::string(100, '\0'); //make a longer string
      return Put(key, writeValue);
    }
  } 
  else {
    Debug("outstandingOpCount :%d", outstandingOpCount);
    std::cerr << "unnecessary transaction is made" << std::endl;
  }
}

} // namespace ycsb
