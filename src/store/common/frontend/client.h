// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * common/client.h:
 *   Interface for a multiple shard transactional client.
 *
 **********************************************************************/

#ifndef _CLIENT_API_H_
#define _CLIENT_API_H_

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/stats.h"
#include "store/common/timestamp.h"
#include "store/common/partitioner.h"
#include "store/common/random.h"
#include "store/common/zipf.h"

#include <functional>
#include <string>
#include <vector>

enum transaction_status_t {
  COMMITTED = 0,
  ABORTED_USER,
  ABORTED_SYSTEM,
  ABORTED_MAX_RETRIES
};

typedef std::function<void(uint64_t)> begin_callback;
typedef std::function<void(uint64_t, Xoroshiro128Plus &, FastZipf &)> begin_callback_ycsb;
typedef std::function<void(uint64_t, uint64_t, uint64_t, Xoroshiro128Plus &, FastZipf &, std::vector<int>)> begin_callback_batch;

typedef std::function<void()> begin_timeout_callback;

typedef std::function<void(int, const std::string &,
    const std::string &, Timestamp)> get_callback;

typedef std::function<void(int, const std::string &,
    const std::string &, Timestamp, Xoroshiro128Plus &, FastZipf &)> get_callback_ycsb;

typedef std::function<void(int, const std::string &)> get_timeout_callback;


typedef std::function<void(int, std::vector<std::string>, std::vector<get_callback>, uint32_t)> get_timeout_callback_batch;

typedef std::function<void(int, const std::string &,
    const std::string &)> put_callback;

typedef std::function<void(int, const std::string &,
    const std::string &, Xoroshiro128Plus &, FastZipf &)> put_callback_ycsb;

typedef std::function<void(int, const std::string &,
    const std::string &)> put_timeout_callback;

typedef std::function<void(transaction_status_t)> commit_callback;

typedef std::function<void(transaction_status_t, int)> commit_callback_batch;

typedef std::function<void()> commit_timeout_callback;

typedef std::function<void()> abort_callback;
typedef std::function<void()> abort_timeout_callback;

class Stats;

class Client {
 public:
  Client() {};
  virtual ~Client() {};

  // Begin a transaction.
  virtual void Begin(begin_callback bcb, begin_timeout_callback btcb,
      uint32_t timeout, bool retry = false) = 0;

  virtual void Begin_ycsb(begin_callback_ycsb bcby, begin_timeout_callback btcb,
      uint32_t timeout, bool retry = false) = 0;

  virtual void Begin_batch(begin_callback_batch bcb, begin_timeout_callback btcb,
      uint32_t timeout, bool retry = false) = 0;

  // Get the value corresponding to key.
  virtual void Get(const std::string &key, get_callback gcb,
      get_timeout_callback gtcb, uint32_t timeout) = 0;

  virtual void Get_ycsb(const std::string &key, get_callback_ycsb gcby,
      get_timeout_callback gtcb, uint32_t timeout) = 0;

  //追加
  virtual void Get_batch(const std::vector<std::string>& key_list, std::vector<get_callback>& gcb_list, std::multimap<std::string, int> *keyTxMap,
      get_timeout_callback_batch gtcb, uint32_t timeout) = 0;

  // Set the value for the given key.
  virtual void Put(const std::string &key, const std::string &value,
      put_callback pcb, put_timeout_callback ptcb, uint32_t timeout) = 0;
  
  virtual void Put_ycsb(const std::string &key, const std::string &value,
      put_callback_ycsb pcby, put_timeout_callback ptcb, uint32_t timeout) = 0;

  virtual void Put_batch(const std::string &key, const std::string &value,
      put_callback pcb, put_timeout_callback ptcb, int batch_size, uint32_t timeout) = 0;

  // Commit all Get(s) and Put(s) since Begin().
  virtual void Commit(commit_callback cc, commit_timeout_callback ctcb,
      uint32_t timeout) = 0;

  virtual void Commit_batch(commit_callback_batch ccb, commit_timeout_callback ctcb,
      uint32_t timeout, int batch_size_at_commit) = 0;

  // Abort all Get(s) and Put(s) since Begin().
  virtual void Abort(abort_callback acb, abort_timeout_callback atcb,
      uint32_t timeout) = 0;

  inline const Stats &GetStats() const { return stats; }

 protected:
  Stats stats;
};

#endif /* _CLIENT_API_H_ */
