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
#ifndef YCSB_SYNC_TRANSACTION_H
#define YCSB_SYNC_TRANSACTION_H

#include <vector>

#include "store/common/frontend/sync_transaction.h"
#include "store/common/frontend/client.h"
#include "store/benchmark/async/common/key_selector.h"
#include "store/benchmark/async/sync_transaction_bench_client.h"


namespace ycsb {

class SyncYCSBTransaction : public SyncTransaction {
 public:
  SyncYCSBTransaction(KeySelector *keySelector, int numOps, bool readOnly, int batchSize, int readRatio, int numKeys, std::mt19937 &rand);
  virtual ~SyncYCSBTransaction();

  virtual Operation GetNextOperation(size_t outstandingOpCount, size_t finishedOpCount,
      std::map<std::string, std::string> readValues);
  
  virtual Operation GetNextOperation_batch(size_t outstandingOpCount, size_t finishedOpCount,
      std::map<std::string, std::string> readValues, int batchSize, Xoroshiro128Plus &rnd, FastZipf &zipf);
  
  virtual transaction_status_t Execute(SyncClient &client);

  virtual std::vector<transaction_status_t> Execute_batch(SyncClient &client);

  
  inline const std::vector<int> getKeyIdxs() const {
    return keyIdxs;
  }
  
 protected:
  
  inline const std::string &GetKey(int i) const {
    return keySelector->GetKey(keyIdxs[i]);
  }
  

  inline const size_t GetNumOps() const { return numOps; }

  KeySelector *keySelector;

 private:
  const size_t numOps;
  const size_t numKeys;
  const bool readOnly;
  const size_t batchSize;
  const size_t readRatio;
  std::vector<int> keyIdxs;
  std::vector<Operation> read_set;
  std::vector<Operation> pre_read_set;
  std::vector<Operation> write_set;
  std::vector<Operation> pre_write_set;

};

}

#endif /*YCSB_SYNC_TRANSACTION_H */
