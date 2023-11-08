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
#include "store/benchmark/async/ycsb_sync/ycsb_sync_client.h"

#include <iostream>

namespace ycsb {

SyncYCSBClient::SyncYCSBClient(KeySelector *keySelector, uint64_t numOps, bool readOnly,
    SyncClient &client,
    Transport &transport, uint64_t id, int numRequests, int expDuration,
    uint64_t delay, int warmupSec, int cooldownSec, int tputInterval,
    uint32_t abortBackoff, bool retryAborted, uint32_t maxBackoff, uint32_t maxAttempts, uint64_t timeout,
    int32_t batchSize, int32_t readRatio, uint64_t numKeys, const std::string &latencyFilename)
    : SyncTransactionBenchClient(client, transport, id, numRequests,
        expDuration, delay, warmupSec, cooldownSec, tputInterval, abortBackoff,
        retryAborted, maxBackoff, maxAttempts, timeout, latencyFilename), keySelector(keySelector),
        numOps(numOps), readOnly(readOnly), batchSize(batchSize), readRatio(readRatio), numKeys(numKeys){
}

SyncYCSBClient::~SyncYCSBClient() {
}

SyncTransaction *SyncYCSBClient::GetNextTransaction() {
  SyncYCSBTransaction *ycsb_tx = new SyncYCSBTransaction(keySelector, numOps, readOnly, batchSize, readRatio, numKeys, GetRand());
  // for(int key : rw_tx->getKeyIdxs()){
  //   //key_counts[key]++;
  //   stats.IncrementList("key distribution", key, 1);
  // }
  return ycsb_tx;
}

std::string SyncYCSBClient::GetLastOp() const {
  return "ycsb";
}

} //namespace ycsb


