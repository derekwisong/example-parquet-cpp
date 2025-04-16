#include <memory>
#include <iostream>

#include <parquet/arrow/writer.h>
#include <arrow/io/file.h>
#include <arrow/api.h>

int main() {
    arrow::FieldVector fields = {
        arrow::field("int_field", arrow::int64()),
        arrow::field("float_field", arrow::float64()),
        arrow::field("string_field", arrow::utf8()),
        arrow::field("bool_field", arrow::boolean()),
    };

    auto schema = std::make_shared<arrow::Schema>(fields);
    std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;

    arrow::Int64Builder int_builder;
    arrow::DoubleBuilder double_builder;
    arrow::StringBuilder string_builder;
    arrow::BooleanBuilder bool_builder;

    // Append some data to the builders
    for (int i = 0; i < 10; ++i) {
        arrow::Status status;
        status = int_builder.Append(i);
        if (!status.ok()) {
            std::cerr << "Error appending to int_builder: " << status.ToString() << std::endl;
            return -1;
        }
        
        status = double_builder.Append(static_cast<double>(i) * 1.1f);
        if (!status.ok()) {
            std::cerr << "Error appending to float_builder: " << status.ToString() << std::endl;
            return -1;
        }

        status = string_builder.Append("string_" + std::to_string(i));
        if (!status.ok()) {
            std::cerr << "Error appending to string_builder: " << status.ToString() << std::endl;
            return -1;
        }

        status = bool_builder.Append(i % 2 == 0);
        if (!status.ok()) {
            std::cerr << "Error appending to bool_builder: " << status.ToString() << std::endl;
            return -1;
        }
    }
    // Finalize the builders to create arrays
    std::shared_ptr<arrow::Array> int_array;
    std::shared_ptr<arrow::Array> float_array;
    std::shared_ptr<arrow::Array> string_array;
    std::shared_ptr<arrow::Array> bool_array;

    arrow::Status status;
    status = int_builder.Finish(&int_array);
    if (!status.ok()) {
        std::cerr << "Error finishing int_builder: " << status.ToString() << std::endl;
        return -1;
    }
    status = double_builder.Finish(&float_array);
    if (!status.ok()) {
        std::cerr << "Error finishing float_builder: " << status.ToString() << std::endl;
        return -1;
    }
    status = string_builder.Finish(&string_array);
    if (!status.ok()) {
        std::cerr << "Error finishing string_builder: " << status.ToString() << std::endl;
        return -1;
    }
    status = bool_builder.Finish(&bool_array);
    if (!status.ok()) {
        std::cerr << "Error finishing bool_builder: " << status.ToString() << std::endl;
        return -1;
    }

    // Create ChunkedArrays from the arrays
    std::shared_ptr<arrow::ChunkedArray> int_chunked_array = std::make_shared<arrow::ChunkedArray>(int_array);
    std::shared_ptr<arrow::ChunkedArray> float_chunked_array = std::make_shared<arrow::ChunkedArray>(float_array);
    std::shared_ptr<arrow::ChunkedArray> string_chunked_array = std::make_shared<arrow::ChunkedArray>(string_array);
    std::shared_ptr<arrow::ChunkedArray> bool_chunked_array = std::make_shared<arrow::ChunkedArray>(bool_array);

    // Add the chunked arrays to the columns vector
    columns.push_back(int_chunked_array);
    columns.push_back(float_chunked_array);
    columns.push_back(string_chunked_array);
    columns.push_back(bool_chunked_array);

    // Create a table from the schema and columns
    auto table = arrow::Table::Make(schema, columns);
    std::cout << "Length of table is: " << table->num_rows() << std::endl;
    std::cout << "Number of columns in table is: " << table->num_columns() << std::endl;
    std::cout << "Schema of table is: " << table->schema()->ToString() << std::endl;

    // Write the table to a Parquet file
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    auto result = arrow::io::FileOutputStream::Open("output.parquet");
    outfile = *result;

    constexpr int CHUNKSIZE = 1024 * 1024 * 5;  // 5 MB
    auto write_result = parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, CHUNKSIZE);
    if (!write_result.ok()) {
        std::cerr << "Error writing table to Parquet file: " << write_result.ToString() << std::endl;
        return -1;
    }
}