#pragma once

#include "../storage/storage.h"

// Query function declarations

void execute_q1(const LineitemTable& lineitem);

void execute_q3(const CustomerTable& customer,
                const OrdersTable& orders,
                const LineitemTable& lineitem);

void execute_q6(const LineitemTable& lineitem);
