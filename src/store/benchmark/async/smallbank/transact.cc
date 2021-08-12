#include "store/benchmark/async/smallbank/transact.h"
#include "store/benchmark/async/smallbank/smallbank_transaction.h"
#include "store/benchmark/async/smallbank/utils.h"

namespace smallbank {

TransactSaving::TransactSaving(const std::string &cust, const int32_t value, const uint32_t timeout) : SmallbankTransaction(TRANSACT), cust(cust), value(value), timeout(timeout) {}
TransactSaving::~TransactSaving() {
}
transaction_status_t TransactSaving::Execute(SyncClient &client) {
	proto::SavingRow savingRow;
    proto::AccountRow accountRow;

    client.Begin(timeout);
    Debug("TransactSaving for name %s with val %d", cust.c_str(), value);
    if (!ReadAccountRow(client, cust, accountRow, timeout)) {
        client.Abort(timeout);
        Debug("Aborted TransactSaving (AccountRow)");
        return ABORTED_USER;
    }
    const uint32_t customerId = accountRow.customer_id();
    if (!ReadSavingRow(client, customerId, savingRow, timeout)) {
        client.Abort(timeout);
        Debug("Aborted TransactSaving (SavingRow)");
        return ABORTED_USER;
    }
    const int32_t balance = savingRow.saving_balance();
    Debug("TransactSaving old value %d", balance);
    const int resultingBalance = balance + value;
    Debug("TransactSaving resulting %d", resultingBalance);
    if (resultingBalance < 0) {
        client.Abort(timeout);
        Debug("Aborted TransactSaving (Negative Result)");
        return ABORTED_USER;
    }
    InsertSavingRow(client, customerId, resultingBalance, timeout);
    return client.Commit(timeout);
}

} // namespace smallbank