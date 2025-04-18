#include <iostream>
#include <memory>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>

namespace {

// Build an Arrow array from a vector of values
template <typename Builder, typename Container>
std::shared_ptr<arrow::Array> build_array(const Container& data) {
    Builder builder;
    arrow::Status status = builder.AppendValues(data);
    if (!status.ok()) {
        std::cerr << "Error appending values: " << status.ToString() << std::endl;
        return nullptr;
    }
    std::shared_ptr<arrow::Array> array;
    status = builder.Finish(&array);
    if (!status.ok()) {
        std::cerr << "Error finishing builder: " << status.ToString() << std::endl;
        return nullptr;
    }
    return array;
}

// Write an Arrow table to a Parquet file
bool write_parquet(const arrow::Table& table, const std::string& path, int64_t chunk_size = 1024 * 1024 * 5) {
    auto result = arrow::io::FileOutputStream::Open(path);
    if (!result.ok()) {
        std::cerr << "Error opening file for writing: " << result.status().ToString() << std::endl;
        return false;
    }
    std::shared_ptr<arrow::io::FileOutputStream> outfile = *result;

    auto write_result = parquet::arrow::WriteTable(table, arrow::default_memory_pool(), outfile, chunk_size);
    if (!write_result.ok()) {
        std::cerr << "Error writing table to Parquet file: " << write_result.ToString() << std::endl;
        return false;
    }

    return true;
}

} // namespace

int main() {
    // Create data
    const std::vector<double> double_values = {1.1, 2.2, 3.3, 4.4, 5.5};
    const std::vector<int64_t> int_values = {1, 2, 3, 4, 5};
    const std::vector<std::string> string_values = {"one", "two", "three", "four", "five"};
    const std::vector<bool> bool_values = {true, false, true, false, true};

    // Define the schema for the table
    const auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{
        arrow::field("int_field", arrow::int64()),
        arrow::field("float_field", arrow::float64()),
        arrow::field("string_field", arrow::utf8()),
        arrow::field("bool_field", arrow::boolean()),
    });

    // Create a vector of column arrays
    arrow::ArrayVector columns({
        build_array<arrow::Int64Builder>(int_values),
        build_array<arrow::DoubleBuilder>(double_values),
        build_array<arrow::StringBuilder>(string_values),
        build_array<arrow::BooleanBuilder>(bool_values),
    });

    // Create a table from the schema and columns
    const auto table = arrow::Table::Make(schema, columns);

    std::cerr << "Length of table is: " << table->num_rows() << std::endl;
    std::cerr << "Number of columns in table is: " << table->num_columns() << std::endl;
    std::cerr << "Schema of table is:\n" << table->schema()->ToString() << std::endl;

    // Write the table to a Parquet file
    constexpr auto file_name = "output.parquet";
    if (write_parquet(*table, file_name)) {
        std::cerr << "Parquet file written successfully." << std::endl;
    } else {
        std::cerr << "Failed to write Parquet file." << std::endl;
        return 1;
    }
}
