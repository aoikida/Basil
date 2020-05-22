#include "store/indicusstore/localbatchverifier.h"

#include "lib/crypto.h"
#include "lib/batched_sigs.h"
#include "store/indicusstore/common.h"
#include "lib/message.h"

namespace indicusstore {

LocalBatchVerifier::LocalBatchVerifier(Stats &stats) : stats(stats) {
  _Latency_Init(&lat, "merkle");
}

LocalBatchVerifier::~LocalBatchVerifier() {
  Latency_Dump(&lat);
}

bool LocalBatchVerifier::Verify(crypto::PubKey *publicKey, const std::string &message,
    const std::string &signature) {
  std::string hashStr;
  std::string rootSig;
  Latency_Start(&lat);
  BatchedSigs::computeBatchedSignatureHash(&signature, &message, publicKey,
      hashStr, rootSig);
  Latency_End(&lat);
  auto itr = cache.find(rootSig);
  if (itr == cache.end()) {
    stats.Increment("verify_cache_miss");
    if (crypto::Verify(publicKey, &hashStr[0], hashStr.length(), &rootSig[0])) {
      cache[rootSig] = hashStr;
      return true;
    } else {
      Debug("Verification with public key failed.");
      return false;
    }
  } else {
    if (hashStr == itr->second) {
      stats.Increment("verify_cache_hit");
      return true;
    } else {
      Debug("Verification via cached hash %s failed.",
          BytesToHex(itr->second, 100).c_str());
      return false;
    }
  }
}

} // namespace indicusstore
