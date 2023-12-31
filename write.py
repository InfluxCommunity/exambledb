import json
import io
from pyarrow.flight import FlightDescriptor, FlightClient
import pyarrow as pa

data = [{"col1":3, "col2":"one"},
        {"col1":3, "col2":"two"}]

table = pa.Table.from_pylist(data)

descriptor = FlightDescriptor.for_path("mytable")
client = FlightClient("grpc://localhost:8081")

writer, _ = client.do_put(descriptor, table.schema)
writer.write_table(table)
print(f"wrote: {table}")
writer.close()
