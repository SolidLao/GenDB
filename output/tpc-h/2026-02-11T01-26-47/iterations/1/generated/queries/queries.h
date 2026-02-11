#pragma once

#include "../storage/storage.h"

namespace gendb {

// Query execution functions
void execute_q1(const LineitemTable& lineitem);
void execute_q3(const LineitemTable& lineitem, const OrdersTable& orders, const CustomerTable& customer);
void execute_q6(const LineitemTable& lineitem);

} // namespace gendb
