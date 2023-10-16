A python example for creating your own online database using Apache Arrow.

1. A Flask server that offers a /write endpoint
2. A Flight server for sending queries
3. Datafusion for process sql queries
4. Saves the data in parquet files

This example is not efficient for featureful. It is designed to inspire developers to use the Apache Arrow project to write their own datatbases. It's like a "hello world" of databases.