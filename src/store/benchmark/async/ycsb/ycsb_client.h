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
#ifndef YCSB_CLIENT_H
#define YCSB_CLIENT_H

#include "store/benchmark/async/async_transaction_bench_client.h"
#include "store/benchmark/async/ycsb/ycsb_transaction.h"
#include "store/benchmark/async/common/key_selector.h"
#include <unordered_map>

namespace ycsb {

enum KeySelection {
  UNIFORM,
  ZIPF
};

class YCSBClient : public AsyncTransactionBenchClient {
 public:
  YCSBClient(KeySelector *keySelector, uint64_t numOps, bool readOnly, AsyncClient &client,
      Transport &transport, uint64_t id, int numRequests, int expDuration,
      uint64_t delay, int warmupSec, int cooldownSec, int tputInterval,
      uint32_t abortBackoff, bool retryAborted, uint32_t maxBackoff, uint32_t maxAttempts,
      int32_t batchSize, int32_t readRatio, uint64_t numKeys, const std::string &latencyFilename = "");


  virtual ~YCSBClient();

  std::unordered_map<int, int> key_counts;

 protected:
  virtual AsyncTransaction *GetNextTransaction();
  virtual std::string GetLastOp() const;

 private:
  KeySelector *keySelector;
  uint64_t numOps;
  uint64_t numKeys;
  uint64_t tid = 0;
  bool readOnly;
  int32_t batchSize;
  int32_t readRatio;

};

} //namespace ycsb

#endif /* YCSB_CLIENT_H */
