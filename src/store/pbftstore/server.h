#ifndef _PBFT_SERVER_H_
#define _PBFT_SERVER_H_

#include <memory>
#include <map>
#include <unordered_map>

#include "store/pbftstore/app.h"
#include "store/pbftstore/server-proto.pb.h"
#include "store/server.h"
#include "lib/keymanager.h"
#include "store/common/backend/versionstore.h"
#include "store/common/partitioner.h"
#include "store/common/truetime.h"

namespace pbftstore {

class Server : public App, public ::Server {
public:
  Server(KeyManager *keyManager, int groupIdx, int myId, int numShards, int numGroups, bool signMessages, bool validateReads, uint64_t timeDelta, partitioner part, TrueTime timeServer = TrueTime(0, 0));
  ~Server();

  ::google::protobuf::Message* Execute(const std::string& type, const std::string& msg, proto::CommitProof &&commitProof);
  ::google::protobuf::Message* HandleMessage(const std::string& type, const std::string& msg);

  void Load(const std::string &key, const std::string &value,
      const Timestamp timestamp);

  Stats &GetStats();

private:
  Stats stats;
  KeyManager* keyManager;
  int groupIdx;
  int myId;
  int numShards;
  int numGroups;
  bool signMessages;
  bool validateReads;
  uint64_t timeDelta;
  partitioner part;
  TrueTime timeServer;

  struct ValueAndProof {
    std::string value;
    std::shared_ptr<proto::CommitProof> commitProof;
  };

  VersionedKVStore<Timestamp, ValueAndProof> commitStore;

  // map from tx digest to transaction
  std::unordered_map<std::string, proto::Transaction> pendingTransactions;

  // map from tx digest to commit proof ptr
  std::unordered_map<std::string, std::shared_ptr<proto::CommitProof>> commitProofs;

  // map from key to ordered map of read timestamp to committed timestamps
  // so if a transaction with timestamp 5 reads version 3 of key A, we have A -> 3 -> 5
  std::unordered_map<std::string, std::map<Timestamp, Timestamp>> committedReads;

  bool CCC(const proto::Transaction& txn);

  // return true if the grouped decision is valid
  bool verifyGDecision(const proto::GroupedDecision& gdecision);

  // return true if this key is owned by this shard
  inline bool IsKeyOwned(const std::string &key) const {
    return static_cast<int>(part(key, numShards) % numGroups) == groupIdx;
  }
};

}

#endif
