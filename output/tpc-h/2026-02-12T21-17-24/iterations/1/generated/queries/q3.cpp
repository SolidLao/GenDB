#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/compute/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>

#include <iostream>
#include <fstream>
#include <chrono>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <iomanip>
#include <ctime>

// Date conversion helpers
inline int32_t date_to_days(int year, int month, int day) {
    int dpm[] = {0,31,59,90,120,151,181,212,243,273,304,334};
    int y = year - 1970;
    int d = y * 365 + y / 4 + dpm[month-1] + day - 1;
    if (month > 2 && (year%4==0 && (year%100!=0 || year%400==0))) d++;
    return d;
}

inline std::string days_to_date_str(int32_t days) {
    struct tm t = {};
    time_t secs = static_cast<time_t>(days) * 86400;
    gmtime_r(&secs, &t);
    char buf[11];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", t.tm_year+1900, t.tm_mon+1, t.tm_mday);
    return buf;
}

// Aggregation key
struct GroupKey {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator==(const GroupKey& other) const {
        return l_orderkey == other.l_orderkey &&
               o_orderdate == other.o_orderdate &&
               o_shippriority == other.o_shippriority;
    }
};

struct GroupKeyHash {
    std::size_t operator()(const GroupKey& k) const {
        return std::hash<int32_t>()(k.l_orderkey) ^
               (std::hash<int32_t>()(k.o_orderdate) << 1) ^
               (std::hash<int32_t>()(k.o_shippriority) << 2);
    }
};

void run_q3(const std::string& parquet_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Step 1: Read and filter customer table
    // Filter: c_mktsegment = 'BUILDING'
    // Project: c_custkey, c_mktsegment
    std::string customer_path = parquet_dir + "/customer.parquet";
    std::shared_ptr<arrow::io::ReadableFile> customer_file;
    PARQUET_ASSIGN_OR_THROW(customer_file, arrow::io::ReadableFile::Open(customer_path));

    std::unique_ptr<parquet::arrow::FileReader> customer_reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(customer_file, arrow::default_memory_pool(), &customer_reader));

    std::shared_ptr<arrow::Schema> customer_schema;
    PARQUET_THROW_NOT_OK(customer_reader->GetSchema(&customer_schema));

    std::vector<int> customer_col_indices = {
        customer_schema->GetFieldIndex("c_custkey"),
        customer_schema->GetFieldIndex("c_mktsegment")
    };

    std::shared_ptr<arrow::Table> customer_table;
    PARQUET_THROW_NOT_OK(customer_reader->ReadTable(customer_col_indices, &customer_table));

    // Filter customer by c_mktsegment = 'BUILDING'
    auto c_mktsegment = arrow::Concatenate(customer_table->GetColumnByName("c_mktsegment")->chunks()).ValueOrDie();
    auto building_scalar = arrow::MakeScalar(arrow::utf8(), std::string("BUILDING")).ValueOrDie();
    std::vector<arrow::Datum> equal_args = {arrow::Datum(c_mktsegment), arrow::Datum(building_scalar)};
    auto customer_mask = arrow::compute::CallFunction("equal", equal_args).ValueOrDie().make_array();
    std::vector<arrow::Datum> filter_args = {arrow::Datum(customer_table), arrow::Datum(customer_mask)};
    auto filtered_customer = arrow::compute::CallFunction("filter", filter_args).ValueOrDie().table();

    // Step 2: Read and filter orders table
    // Filter: o_orderdate < '1995-03-15'
    // Project: o_orderkey, o_custkey, o_orderdate, o_shippriority
    std::string orders_path = parquet_dir + "/orders.parquet";
    std::shared_ptr<arrow::io::ReadableFile> orders_file;
    PARQUET_ASSIGN_OR_THROW(orders_file, arrow::io::ReadableFile::Open(orders_path));

    std::unique_ptr<parquet::arrow::FileReader> orders_reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(orders_file, arrow::default_memory_pool(), &orders_reader));

    std::shared_ptr<arrow::Schema> orders_schema;
    PARQUET_THROW_NOT_OK(orders_reader->GetSchema(&orders_schema));

    std::vector<int> orders_col_indices = {
        orders_schema->GetFieldIndex("o_orderkey"),
        orders_schema->GetFieldIndex("o_custkey"),
        orders_schema->GetFieldIndex("o_orderdate"),
        orders_schema->GetFieldIndex("o_shippriority")
    };

    std::shared_ptr<arrow::Table> orders_table;
    PARQUET_THROW_NOT_OK(orders_reader->ReadTable(orders_col_indices, &orders_table));

    // Filter orders by o_orderdate < '1995-03-15'
    int32_t date_1995_03_15 = date_to_days(1995, 3, 15);
    auto o_orderdate = arrow::Concatenate(orders_table->GetColumnByName("o_orderdate")->chunks()).ValueOrDie();
    auto date_scalar = arrow::MakeScalar(arrow::date32(), date_1995_03_15).ValueOrDie();
    std::vector<arrow::Datum> less_args = {arrow::Datum(o_orderdate), arrow::Datum(date_scalar)};
    auto orders_mask = arrow::compute::CallFunction("less", less_args).ValueOrDie().make_array();
    std::vector<arrow::Datum> filter_orders_args = {arrow::Datum(orders_table), arrow::Datum(orders_mask)};
    auto filtered_orders = arrow::compute::CallFunction("filter", filter_orders_args).ValueOrDie().table();

    // Step 3: Join customer with orders on c_custkey = o_custkey
    // Build hash table on filtered customer (smaller)
    auto c_custkey_arr = std::static_pointer_cast<arrow::Int32Array>(
        arrow::Concatenate(filtered_customer->GetColumnByName("c_custkey")->chunks()).ValueOrDie());

    std::unordered_map<int32_t, std::vector<int64_t>> customer_ht;
    for (int64_t i = 0; i < c_custkey_arr->length(); ++i) {
        if (!c_custkey_arr->IsNull(i)) {
            customer_ht[c_custkey_arr->Value(i)].push_back(i);
        }
    }

    // Probe with filtered orders
    auto o_custkey_arr = std::static_pointer_cast<arrow::Int32Array>(
        arrow::Concatenate(filtered_orders->GetColumnByName("o_custkey")->chunks()).ValueOrDie());

    arrow::Int64Builder orders_idx_builder;
    for (int64_t i = 0; i < o_custkey_arr->length(); ++i) {
        if (o_custkey_arr->IsNull(i)) continue;
        auto it = customer_ht.find(o_custkey_arr->Value(i));
        if (it != customer_ht.end()) {
            // Each matching customer row joins with this order row
            for (size_t j = 0; j < it->second.size(); ++j) {
                PARQUET_THROW_NOT_OK(orders_idx_builder.Append(i));
            }
        }
    }

    std::shared_ptr<arrow::Array> orders_indices;
    PARQUET_THROW_NOT_OK(orders_idx_builder.Finish(&orders_indices));

    // Take from filtered_orders to get the joined result
    arrow::FieldVector co_fields;
    arrow::ArrayVector co_arrays;

    for (int c = 0; c < filtered_orders->num_columns(); ++c) {
        auto col = arrow::Concatenate(filtered_orders->column(c)->chunks()).ValueOrDie();
        std::vector<arrow::Datum> take_args = {arrow::Datum(col), arrow::Datum(orders_indices)};
        auto taken = arrow::compute::CallFunction("take", take_args).ValueOrDie().make_array();
        co_fields.push_back(filtered_orders->schema()->field(c));
        co_arrays.push_back(taken);
    }

    auto customer_orders = arrow::Table::Make(arrow::schema(co_fields), co_arrays);

    // Step 4: Read and filter lineitem table
    // Filter: l_shipdate > '1995-03-15'
    // Project: l_orderkey, l_extendedprice, l_discount, l_shipdate
    std::string lineitem_path = parquet_dir + "/lineitem.parquet";
    std::shared_ptr<arrow::io::ReadableFile> lineitem_file;
    PARQUET_ASSIGN_OR_THROW(lineitem_file, arrow::io::ReadableFile::Open(lineitem_path));

    std::unique_ptr<parquet::arrow::FileReader> lineitem_reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(lineitem_file, arrow::default_memory_pool(), &lineitem_reader));

    std::shared_ptr<arrow::Schema> lineitem_schema;
    PARQUET_THROW_NOT_OK(lineitem_reader->GetSchema(&lineitem_schema));

    std::vector<int> lineitem_col_indices = {
        lineitem_schema->GetFieldIndex("l_orderkey"),
        lineitem_schema->GetFieldIndex("l_extendedprice"),
        lineitem_schema->GetFieldIndex("l_discount"),
        lineitem_schema->GetFieldIndex("l_shipdate")
    };

    std::shared_ptr<arrow::Table> lineitem_table;
    PARQUET_THROW_NOT_OK(lineitem_reader->ReadTable(lineitem_col_indices, &lineitem_table));

    // Filter lineitem by l_shipdate > '1995-03-15'
    auto l_shipdate = arrow::Concatenate(lineitem_table->GetColumnByName("l_shipdate")->chunks()).ValueOrDie();
    std::vector<arrow::Datum> greater_args = {arrow::Datum(l_shipdate), arrow::Datum(date_scalar)};
    auto lineitem_mask = arrow::compute::CallFunction("greater", greater_args).ValueOrDie().make_array();
    std::vector<arrow::Datum> filter_lineitem_args = {arrow::Datum(lineitem_table), arrow::Datum(lineitem_mask)};
    auto filtered_lineitem = arrow::compute::CallFunction("filter", filter_lineitem_args).ValueOrDie().table();

    // Step 5: Join customer_orders with lineitem on o_orderkey = l_orderkey
    // Build hash table on customer_orders (project o_orderkey, o_orderdate, o_shippriority)
    auto co_orderkey_arr = std::static_pointer_cast<arrow::Int32Array>(
        arrow::Concatenate(customer_orders->GetColumnByName("o_orderkey")->chunks()).ValueOrDie());

    std::unordered_map<int32_t, std::vector<int64_t>> co_ht;
    for (int64_t i = 0; i < co_orderkey_arr->length(); ++i) {
        if (!co_orderkey_arr->IsNull(i)) {
            co_ht[co_orderkey_arr->Value(i)].push_back(i);
        }
    }

    // Probe with filtered lineitem
    auto l_orderkey_arr = std::static_pointer_cast<arrow::Int32Array>(
        arrow::Concatenate(filtered_lineitem->GetColumnByName("l_orderkey")->chunks()).ValueOrDie());

    arrow::Int64Builder co_idx_builder, lineitem_idx_builder;
    for (int64_t i = 0; i < l_orderkey_arr->length(); ++i) {
        if (l_orderkey_arr->IsNull(i)) continue;
        auto it = co_ht.find(l_orderkey_arr->Value(i));
        if (it != co_ht.end()) {
            for (int64_t j : it->second) {
                PARQUET_THROW_NOT_OK(co_idx_builder.Append(j));
                PARQUET_THROW_NOT_OK(lineitem_idx_builder.Append(i));
            }
        }
    }

    std::shared_ptr<arrow::Array> co_indices, lineitem_indices;
    PARQUET_THROW_NOT_OK(co_idx_builder.Finish(&co_indices));
    PARQUET_THROW_NOT_OK(lineitem_idx_builder.Finish(&lineitem_indices));

    // Take from both sides
    arrow::FieldVector join_fields;
    arrow::ArrayVector join_arrays;

    for (int c = 0; c < customer_orders->num_columns(); ++c) {
        auto col = arrow::Concatenate(customer_orders->column(c)->chunks()).ValueOrDie();
        std::vector<arrow::Datum> take_co_args = {arrow::Datum(col), arrow::Datum(co_indices)};
        auto taken = arrow::compute::CallFunction("take", take_co_args).ValueOrDie().make_array();
        join_fields.push_back(customer_orders->schema()->field(c));
        join_arrays.push_back(taken);
    }

    for (int c = 0; c < filtered_lineitem->num_columns(); ++c) {
        auto col = arrow::Concatenate(filtered_lineitem->column(c)->chunks()).ValueOrDie();
        std::vector<arrow::Datum> take_li_args = {arrow::Datum(col), arrow::Datum(lineitem_indices)};
        auto taken = arrow::compute::CallFunction("take", take_li_args).ValueOrDie().make_array();
        join_fields.push_back(filtered_lineitem->schema()->field(c));
        join_arrays.push_back(taken);
    }

    auto joined = arrow::Table::Make(arrow::schema(join_fields), join_arrays);

    // Step 6: Compute revenue = l_extendedprice * (1 - l_discount)
    // Cast decimal columns to double for computation
    auto l_extendedprice_decimal = arrow::Concatenate(joined->GetColumnByName("l_extendedprice")->chunks()).ValueOrDie();
    auto l_discount_decimal = arrow::Concatenate(joined->GetColumnByName("l_discount")->chunks()).ValueOrDie();

    arrow::compute::CastOptions cast_opts = arrow::compute::CastOptions::Safe(arrow::float64());
    std::vector<arrow::Datum> cast_price_args = {arrow::Datum(l_extendedprice_decimal)};
    auto l_extendedprice_arr = arrow::compute::CallFunction("cast", cast_price_args, &cast_opts).ValueOrDie().make_array();
    std::vector<arrow::Datum> cast_discount_args = {arrow::Datum(l_discount_decimal)};
    auto l_discount_arr = arrow::compute::CallFunction("cast", cast_discount_args, &cast_opts).ValueOrDie().make_array();

    // 1 - l_discount
    auto one = arrow::MakeScalar(arrow::float64(), 1.0).ValueOrDie();
    std::vector<arrow::Datum> subtract_args = {arrow::Datum(one), arrow::Datum(l_discount_arr)};
    auto one_minus_discount = arrow::compute::CallFunction("subtract", subtract_args).ValueOrDie().make_array();

    // l_extendedprice * (1 - l_discount)
    std::vector<arrow::Datum> multiply_args = {arrow::Datum(l_extendedprice_arr), arrow::Datum(one_minus_discount)};
    auto revenue_arr = arrow::compute::CallFunction("multiply", multiply_args).ValueOrDie().make_array();

    // Step 7: Group by l_orderkey, o_orderdate, o_shippriority and aggregate
    auto grouped_l_orderkey = std::static_pointer_cast<arrow::Int32Array>(
        arrow::Concatenate(joined->GetColumnByName("l_orderkey")->chunks()).ValueOrDie());
    auto grouped_o_orderdate = std::static_pointer_cast<arrow::Date32Array>(
        arrow::Concatenate(joined->GetColumnByName("o_orderdate")->chunks()).ValueOrDie());
    auto grouped_o_shippriority = std::static_pointer_cast<arrow::Int32Array>(
        arrow::Concatenate(joined->GetColumnByName("o_shippriority")->chunks()).ValueOrDie());
    auto revenue_dbl = std::static_pointer_cast<arrow::DoubleArray>(revenue_arr);

    std::unordered_map<GroupKey, double, GroupKeyHash> aggregation;

    for (int64_t i = 0; i < grouped_l_orderkey->length(); ++i) {
        GroupKey key;
        key.l_orderkey = grouped_l_orderkey->Value(i);
        key.o_orderdate = grouped_o_orderdate->Value(i);
        key.o_shippriority = grouped_o_shippriority->Value(i);
        aggregation[key] += revenue_dbl->Value(i);
    }

    // Step 8: Convert aggregation to vector and sort
    struct Result {
        int32_t l_orderkey;
        double revenue;
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    std::vector<Result> results;
    results.reserve(aggregation.size());
    for (const auto& kv : aggregation) {
        results.push_back({
            kv.first.l_orderkey,
            kv.second,
            kv.first.o_orderdate,
            kv.first.o_shippriority
        });
    }

    // Sort by revenue DESC, o_orderdate ASC
    std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
        if (a.revenue != b.revenue) return a.revenue > b.revenue;
        return a.o_orderdate < b.o_orderdate;
    });

    // Limit to 10
    if (results.size() > 10) {
        results.resize(10);
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;

    // Step 9: Write results to CSV
    std::string output_path = results_dir + "/Q3.csv";
    std::ofstream out(output_path);
    out << "l_orderkey|revenue|o_orderdate|o_shippriority\n";

    for (const auto& r : results) {
        out << r.l_orderkey << "|"
            << std::fixed << std::setprecision(2) << r.revenue << "|"
            << days_to_date_str(r.o_orderdate) << "|"
            << r.o_shippriority << "\n";
    }

    out.close();

    std::cout << "Q3: " << results.size() << " rows in "
              << elapsed.count() << " seconds" << std::endl;
}
