#ifndef _ASYNC_TRANSACTION_H_
#define _ASYNC_TRANSACTION_H_

#include "store/common/frontend/client.h"
#include "store/common/frontend/transaction_utils.h"

#include <functional>
#include <map>
#include <string>

class AsyncTransaction {
 public:
  AsyncTransaction() { }
  virtual ~AsyncTransaction() { }

  virtual Operation GetNextOperation(size_t opCount,
      const std::map<std::string, std::string> readValues) = 0;

};

#endif
