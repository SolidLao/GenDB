#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/compute/api.h>
#include <arrow/dataset/api.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/filesystem/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/file_reader.h>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <chrono>
#include <cmath>

// Helper: Convert date to days since epoch (1970-01-01)
inline int32_t date_to_days(int year, int month, int day) {
    int dpm[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};
    int y = year - 1970;
    int d = y * 365 + y / 4 + dpm[month - 1] + day - 1;
    if (month > 2 && (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))) d++;
    return d;
}

// Helper: Format date32 for output (YYYY-MM-DD)
inline std::string days_to_date_str(int32_t days) {
    struct tm t = {};
    time_t secs = static_cast<time_t>(days) * 86400;
    gmtime_r(&secs, &t);
    char buf[11];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", t.tm_year + 1900, t.tm_mon + 1, t.tm_mday);
    return buf;
}

void run_q3(const std::string& parquet_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Step 1: Read and filter customer table (c_mktsegment = 'BUILDING')
    std::string customer_path = parquet_dir + "/customer.parquet";
    std::shared_ptr<arrow::io::ReadableFile> customer_file;
    PARQUET_ASSIGN_OR_THROW(customer_file, arrow::io::ReadableFile::Open(customer_path));

    std::unique_ptr<parquet::arrow::FileReader> customer_reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(customer_file, arrow::default_memory_pool(), &customer_reader));

    std::shared_ptr<arrow::Schema> customer_schema;
    PARQUET_THROW_NOT_OK(customer_reader->GetSchema(&customer_schema));

    std::vector<int> customer_cols = {
        customer_schema->GetFieldIndex("c_custkey"),
        customer_schema->GetFieldIndex("c_mktsegment")
    };

    std::shared_ptr<arrow::Table> customer_table;
    PARQUET_THROW_NOT_OK(customer_reader->ReadTable(customer_cols, &customer_table));

    // Filter customer: c_mktsegment = 'BUILDING'
    auto c_mktsegment = arrow::Concatenate(customer_table->GetColumnByName("c_mktsegment")->chunks()).ValueOrDie();
    auto building_scalar = arrow::MakeScalar(arrow::utf8(), std::string("BUILDING")).ValueOrDie();
    std::vector<arrow::Datum> equal_args = {arrow::Datum(c_mktsegment), arrow::Datum(building_scalar)};
    auto customer_mask = arrow::compute::CallFunction("equal", equal_args).ValueOrDie().make_array();
    std::vector<arrow::Datum> filter_args = {arrow::Datum(customer_table), arrow::Datum(customer_mask)};
    auto filtered_customer = arrow::compute::CallFunction("filter", filter_args).ValueOrDie().table();

    // Step 2: Read and filter orders table (o_orderdate < '1995-03-15')
    // Use dataset API for predicate pushdown
    std::string orders_path = parquet_dir + "/orders.parquet";
    int32_t date_1995_03_15 = date_to_days(1995, 3, 15);
    auto date_scalar = arrow::MakeScalar(arrow::date32(), date_1995_03_15).ValueOrDie();

    auto orders_filter_expr = arrow::compute::less(
        arrow::compute::field_ref("o_orderdate"),
        arrow::compute::literal(date_scalar)
    );

    auto orders_fs = std::make_shared<arrow::fs::LocalFileSystem>();
    auto orders_format = std::make_shared<arrow::dataset::ParquetFileFormat>();

    arrow::fs::FileInfoVector orders_file_infos;
    auto orders_file_info = orders_fs->GetFileInfo(orders_path).ValueOrDie();
    orders_file_infos.push_back(orders_file_info);

    auto orders_dataset_factory_result = arrow::dataset::FileSystemDatasetFactory::Make(
        orders_fs, orders_file_infos, orders_format, {}
    );
    if (!orders_dataset_factory_result.ok()) {
        throw std::runtime_error("Failed to create orders dataset factory: " + orders_dataset_factory_result.status().ToString());
    }
    auto orders_dataset_factory = orders_dataset_factory_result.ValueOrDie();

    auto orders_dataset_result = orders_dataset_factory->Finish();
    if (!orders_dataset_result.ok()) {
        throw std::runtime_error("Failed to create orders dataset: " + orders_dataset_result.status().ToString());
    }
    auto orders_dataset = orders_dataset_result.ValueOrDie();

    auto orders_scanner_builder_result = orders_dataset->NewScan();
    if (!orders_scanner_builder_result.ok()) {
        throw std::runtime_error("Failed to create orders scanner builder: " + orders_scanner_builder_result.status().ToString());
    }
    auto orders_scanner_builder = orders_scanner_builder_result.ValueOrDie();

    auto orders_filter_status = orders_scanner_builder->Filter(orders_filter_expr);
    if (!orders_filter_status.ok()) {
        throw std::runtime_error("Failed to set orders filter: " + orders_filter_status.ToString());
    }

    auto orders_project_status = orders_scanner_builder->Project({
        "o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"
    });
    if (!orders_project_status.ok()) {
        throw std::runtime_error("Failed to set orders projection: " + orders_project_status.ToString());
    }

    auto orders_scanner_result = orders_scanner_builder->Finish();
    if (!orders_scanner_result.ok()) {
        throw std::runtime_error("Failed to build orders scanner: " + orders_scanner_result.status().ToString());
    }
    auto orders_scanner = orders_scanner_result.ValueOrDie();

    auto orders_table_result = orders_scanner->ToTable();
    if (!orders_table_result.ok()) {
        throw std::runtime_error("Failed to scan orders table: " + orders_table_result.status().ToString());
    }
    auto filtered_orders = orders_table_result.ValueOrDie();

    // Step 3: Join customer with orders (c_custkey = o_custkey)
    // Build hash table on filtered customer (smaller table)
    auto c_custkey = std::static_pointer_cast<arrow::Int32Array>(
        arrow::Concatenate(filtered_customer->GetColumnByName("c_custkey")->chunks()).ValueOrDie());

    std::unordered_map<int32_t, std::vector<int64_t>> customer_ht;
    for (int64_t i = 0; i < c_custkey->length(); ++i) {
        if (!c_custkey->IsNull(i)) {
            customer_ht[c_custkey->Value(i)].push_back(i);
        }
    }

    // Probe with filtered orders
    auto o_custkey = std::static_pointer_cast<arrow::Int32Array>(
        arrow::Concatenate(filtered_orders->GetColumnByName("o_custkey")->chunks()).ValueOrDie());

    arrow::Int64Builder cust_order_left_idx, cust_order_right_idx;
    for (int64_t i = 0; i < o_custkey->length(); ++i) {
        if (o_custkey->IsNull(i)) continue;
        auto it = customer_ht.find(o_custkey->Value(i));
        if (it != customer_ht.end()) {
            for (int64_t j : it->second) {
                PARQUET_THROW_NOT_OK(cust_order_left_idx.Append(j));
                PARQUET_THROW_NOT_OK(cust_order_right_idx.Append(i));
            }
        }
    }

    std::shared_ptr<arrow::Array> cust_indices, order_indices;
    PARQUET_THROW_NOT_OK(cust_order_left_idx.Finish(&cust_indices));
    PARQUET_THROW_NOT_OK(cust_order_right_idx.Finish(&order_indices));

    // Take columns from orders (we don't need customer columns anymore)
    arrow::FieldVector cust_order_fields;
    arrow::ArrayVector cust_order_arrays;

    for (int c = 0; c < filtered_orders->num_columns(); ++c) {
        auto col = arrow::Concatenate(filtered_orders->column(c)->chunks()).ValueOrDie();
        std::vector<arrow::Datum> take_args = {arrow::Datum(col), arrow::Datum(order_indices)};
        auto taken = arrow::compute::CallFunction("take", take_args).ValueOrDie().make_array();
        cust_order_fields.push_back(filtered_orders->schema()->field(c));
        cust_order_arrays.push_back(taken);
    }

    auto cust_order_joined = arrow::Table::Make(arrow::schema(cust_order_fields), cust_order_arrays);

    // Step 4: Read and filter lineitem table (l_shipdate > '1995-03-15')
    // Use dataset API for predicate pushdown
    std::string lineitem_path = parquet_dir + "/lineitem.parquet";

    auto lineitem_filter_expr = arrow::compute::greater(
        arrow::compute::field_ref("l_shipdate"),
        arrow::compute::literal(date_scalar)
    );

    auto lineitem_fs = std::make_shared<arrow::fs::LocalFileSystem>();
    auto lineitem_format = std::make_shared<arrow::dataset::ParquetFileFormat>();

    arrow::fs::FileInfoVector lineitem_file_infos;
    auto lineitem_file_info = lineitem_fs->GetFileInfo(lineitem_path).ValueOrDie();
    lineitem_file_infos.push_back(lineitem_file_info);

    auto lineitem_dataset_factory_result = arrow::dataset::FileSystemDatasetFactory::Make(
        lineitem_fs, lineitem_file_infos, lineitem_format, {}
    );
    if (!lineitem_dataset_factory_result.ok()) {
        throw std::runtime_error("Failed to create lineitem dataset factory: " + lineitem_dataset_factory_result.status().ToString());
    }
    auto lineitem_dataset_factory = lineitem_dataset_factory_result.ValueOrDie();

    auto lineitem_dataset_result = lineitem_dataset_factory->Finish();
    if (!lineitem_dataset_result.ok()) {
        throw std::runtime_error("Failed to create lineitem dataset: " + lineitem_dataset_result.status().ToString());
    }
    auto lineitem_dataset = lineitem_dataset_result.ValueOrDie();

    auto lineitem_scanner_builder_result = lineitem_dataset->NewScan();
    if (!lineitem_scanner_builder_result.ok()) {
        throw std::runtime_error("Failed to create lineitem scanner builder: " + lineitem_scanner_builder_result.status().ToString());
    }
    auto lineitem_scanner_builder = lineitem_scanner_builder_result.ValueOrDie();

    auto lineitem_filter_status = lineitem_scanner_builder->Filter(lineitem_filter_expr);
    if (!lineitem_filter_status.ok()) {
        throw std::runtime_error("Failed to set lineitem filter: " + lineitem_filter_status.ToString());
    }

    auto lineitem_project_status = lineitem_scanner_builder->Project({
        "l_orderkey", "l_extendedprice", "l_discount"
    });
    if (!lineitem_project_status.ok()) {
        throw std::runtime_error("Failed to set lineitem projection: " + lineitem_project_status.ToString());
    }

    auto lineitem_scanner_result = lineitem_scanner_builder->Finish();
    if (!lineitem_scanner_result.ok()) {
        throw std::runtime_error("Failed to build lineitem scanner: " + lineitem_scanner_result.status().ToString());
    }
    auto lineitem_scanner = lineitem_scanner_result.ValueOrDie();

    auto lineitem_table_result = lineitem_scanner->ToTable();
    if (!lineitem_table_result.ok()) {
        throw std::runtime_error("Failed to scan lineitem table: " + lineitem_table_result.status().ToString());
    }
    auto filtered_lineitem = lineitem_table_result.ValueOrDie();

    // Step 5: Join (customer+orders) with lineitem (o_orderkey = l_orderkey)
    // Build hash table on cust_order_joined (smaller after filters)
    auto o_orderkey = std::static_pointer_cast<arrow::Int32Array>(
        arrow::Concatenate(cust_order_joined->GetColumnByName("o_orderkey")->chunks()).ValueOrDie());

    std::unordered_map<int32_t, std::vector<int64_t>> order_ht;
    for (int64_t i = 0; i < o_orderkey->length(); ++i) {
        if (!o_orderkey->IsNull(i)) {
            order_ht[o_orderkey->Value(i)].push_back(i);
        }
    }

    // Probe with filtered lineitem
    auto l_orderkey = std::static_pointer_cast<arrow::Int32Array>(
        arrow::Concatenate(filtered_lineitem->GetColumnByName("l_orderkey")->chunks()).ValueOrDie());

    arrow::Int64Builder final_left_idx, final_right_idx;
    for (int64_t i = 0; i < l_orderkey->length(); ++i) {
        if (l_orderkey->IsNull(i)) continue;
        auto it = order_ht.find(l_orderkey->Value(i));
        if (it != order_ht.end()) {
            for (int64_t j : it->second) {
                PARQUET_THROW_NOT_OK(final_left_idx.Append(j));
                PARQUET_THROW_NOT_OK(final_right_idx.Append(i));
            }
        }
    }

    std::shared_ptr<arrow::Array> final_order_indices, final_lineitem_indices;
    PARQUET_THROW_NOT_OK(final_left_idx.Finish(&final_order_indices));
    PARQUET_THROW_NOT_OK(final_right_idx.Finish(&final_lineitem_indices));

    // Take columns from both sides
    arrow::FieldVector final_fields;
    arrow::ArrayVector final_arrays;

    for (int c = 0; c < cust_order_joined->num_columns(); ++c) {
        auto col = arrow::Concatenate(cust_order_joined->column(c)->chunks()).ValueOrDie();
        std::vector<arrow::Datum> take_args = {arrow::Datum(col), arrow::Datum(final_order_indices)};
        auto taken = arrow::compute::CallFunction("take", take_args).ValueOrDie().make_array();
        final_fields.push_back(cust_order_joined->schema()->field(c));
        final_arrays.push_back(taken);
    }

    for (int c = 0; c < filtered_lineitem->num_columns(); ++c) {
        auto col = arrow::Concatenate(filtered_lineitem->column(c)->chunks()).ValueOrDie();
        std::vector<arrow::Datum> take_args = {arrow::Datum(col), arrow::Datum(final_lineitem_indices)};
        auto taken = arrow::compute::CallFunction("take", take_args).ValueOrDie().make_array();
        final_fields.push_back(filtered_lineitem->schema()->field(c));
        final_arrays.push_back(taken);
    }

    auto joined = arrow::Table::Make(arrow::schema(final_fields), final_arrays);

    // Step 6: Compute revenue = l_extendedprice * (1 - l_discount)
    auto l_extendedprice_arr = arrow::Concatenate(joined->GetColumnByName("l_extendedprice")->chunks()).ValueOrDie();
    auto l_discount_arr = arrow::Concatenate(joined->GetColumnByName("l_discount")->chunks()).ValueOrDie();

    // Cast decimal to double for arithmetic
    arrow::compute::CastOptions cast_opts = arrow::compute::CastOptions::Safe(arrow::float64());
    std::vector<arrow::Datum> cast_price_args = {arrow::Datum(l_extendedprice_arr)};
    auto price_dbl = arrow::compute::CallFunction("cast", cast_price_args, &cast_opts).ValueOrDie().make_array();
    std::vector<arrow::Datum> cast_discount_args = {arrow::Datum(l_discount_arr)};
    auto discount_dbl = arrow::compute::CallFunction("cast", cast_discount_args, &cast_opts).ValueOrDie().make_array();

    auto one_scalar = arrow::MakeScalar(arrow::float64(), 1.0).ValueOrDie();
    std::vector<arrow::Datum> subtract_args = {arrow::Datum(one_scalar), arrow::Datum(discount_dbl)};
    auto one_minus_discount = arrow::compute::CallFunction("subtract", subtract_args).ValueOrDie().make_array();
    std::vector<arrow::Datum> multiply_args = {arrow::Datum(price_dbl), arrow::Datum(one_minus_discount)};
    auto revenue_arr = arrow::compute::CallFunction("multiply", multiply_args).ValueOrDie().make_array();

    // Add revenue column to joined table
    auto revenue_chunked = std::make_shared<arrow::ChunkedArray>(revenue_arr);
    joined = joined->AddColumn(
        joined->num_columns(),
        arrow::field("revenue", arrow::float64()),
        revenue_chunked).ValueOrDie();

    // Step 7: Group by l_orderkey, o_orderdate, o_shippriority and sum revenue
    // Extract arrays for grouping
    auto joined_l_orderkey = std::static_pointer_cast<arrow::Int32Array>(
        arrow::Concatenate(joined->GetColumnByName("l_orderkey")->chunks()).ValueOrDie());
    auto joined_o_orderdate = std::static_pointer_cast<arrow::Date32Array>(
        arrow::Concatenate(joined->GetColumnByName("o_orderdate")->chunks()).ValueOrDie());
    auto joined_o_shippriority = std::static_pointer_cast<arrow::Int32Array>(
        arrow::Concatenate(joined->GetColumnByName("o_shippriority")->chunks()).ValueOrDie());
    auto joined_revenue = std::static_pointer_cast<arrow::DoubleArray>(revenue_arr);

    // Hash aggregation: group by (l_orderkey, o_orderdate, o_shippriority)
    struct GroupKey {
        int32_t orderkey;
        int32_t orderdate;
        int32_t shippriority;

        bool operator==(const GroupKey& other) const {
            return orderkey == other.orderkey &&
                   orderdate == other.orderdate &&
                   shippriority == other.shippriority;
        }
    };

    struct GroupKeyHash {
        size_t operator()(const GroupKey& k) const {
            return std::hash<int32_t>()(k.orderkey) ^
                   (std::hash<int32_t>()(k.orderdate) << 1) ^
                   (std::hash<int32_t>()(k.shippriority) << 2);
        }
    };

    std::unordered_map<GroupKey, double, GroupKeyHash> groups;

    for (int64_t i = 0; i < joined_l_orderkey->length(); ++i) {
        if (!joined_l_orderkey->IsNull(i) && !joined_o_orderdate->IsNull(i) &&
            !joined_o_shippriority->IsNull(i) && !joined_revenue->IsNull(i)) {
            GroupKey key{
                joined_l_orderkey->Value(i),
                joined_o_orderdate->Value(i),
                joined_o_shippriority->Value(i)
            };
            groups[key] += joined_revenue->Value(i);
        }
    }

    // Step 8: Convert to vectors and sort by revenue DESC, o_orderdate ASC
    struct ResultRow {
        int32_t orderkey;
        double revenue;
        int32_t orderdate;
        int32_t shippriority;
    };

    std::vector<ResultRow> results;
    results.reserve(groups.size());

    for (const auto& kv : groups) {
        results.push_back({
            kv.first.orderkey,
            kv.second,
            kv.first.orderdate,
            kv.first.shippriority
        });
    }

    // CORRECTNESS FIX: Use stable_sort with tie-breakers to ensure deterministic ordering
    // when revenues are equal (floating-point comparison). This prevents missing rows
    // in the top-10 results due to non-deterministic ordering of equal values.
    // Tie-breaker order: revenue DESC, o_orderdate ASC, o_orderkey ASC
    std::stable_sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        // Primary: revenue DESC (use epsilon comparison for floating-point)
        const double epsilon = 1e-9;
        if (std::abs(a.revenue - b.revenue) > epsilon) {
            return a.revenue > b.revenue;
        }
        // Tie-breaker 1: o_orderdate ASC
        if (a.orderdate != b.orderdate) {
            return a.orderdate < b.orderdate;
        }
        // Tie-breaker 2: o_orderkey ASC (ensures full determinism)
        return a.orderkey < b.orderkey;
    });

    // Limit to 10
    if (results.size() > 10) {
        results.resize(10);
    }

    // Step 9: Write results to CSV
    std::string output_path = results_dir + "/Q3.csv";
    std::ofstream out(output_path);
    out << std::fixed << std::setprecision(2);

    for (const auto& row : results) {
        out << row.orderkey << "|"
            << row.revenue << "|"
            << days_to_date_str(row.orderdate) << "|"
            << row.shippriority << "\n";
    }

    out.close();

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    std::cout << "Q3: " << results.size() << " rows, " << duration << " ms\n";
}
