#include "store/benchmark/async/retwis/retwis_client.h"

#include "store/benchmark/async/retwis/add_user.h"
#include "store/benchmark/async/retwis/follow.h"
#include "store/benchmark/async/retwis/get_timeline.h"
#include "store/benchmark/async/retwis/post_tweet.h"

namespace retwis {

RetwisClient::RetwisClient(KeySelector *keySelector, Client &client,
    Transport &transport, int numRequests, uint64_t delay, int warmupSec,
    int tputInterval, const std::string &latencyFilename)
    : BenchmarkClient(client, transport, numRequests, delay, warmupSec,
        tputInterval, latencyFilename), keySelector(keySelector),
      currTxn(nullptr) {
}

RetwisClient::~RetwisClient() {
  if (currTxn != nullptr) {
    //delete currTxn;
  }
}

void RetwisClient::SendNext() {
  if (currTxn != nullptr) {
    //delete currTxn;
  }

  int ttype = std::rand() % 100;
  if (ttype < 5) {
    currTxn = new AddUser(&client, keySelector);
    lastOp = "add_user";
  } else if (ttype < 20) {
    currTxn = new Follow(&client, keySelector);
    lastOp = "follow";
  } else if (ttype < 50) {
    currTxn = new PostTweet(&client, keySelector);
    lastOp = "post_tweet";
  } else {
    currTxn = new GetTimeline(&client, keySelector);
    lastOp = "get_timeline";
  }
  currTxn->Execute([this](bool committed,
      std::map<std::string, std::string> readValues){
    this->OnReply();
  });
}

std::string RetwisClient::GetLastOp() const {
  return lastOp;
}

} //namespace retwis
