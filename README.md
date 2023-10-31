A very minimal python example for creating your own online database using Apache Arrow.

1. do_put() function for receiving arrow tables and appending them to Parquet files
2. do_get() function for receiving SQL queries, executing them with DataFusion and returning results
3. Example ready and write code

This example is not efficient for featureful. It is designed to inspire developers to use the Apache Arrow project to write their own datatbases. It's like a "hello world" of databases.