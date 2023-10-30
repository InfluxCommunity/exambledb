import json
import io
from pyarrow.flight import FlightDescriptor, FlightClient
from pyarrow.json import read_json

data = [
        {"col1":1, "col2":"one"},
        {"col1":1, "col2":"two"}]

# Convert the data to line-delimited JSON format
json_lines = "\n".join([json.dumps(record).encode for record in data])

# Convert the line-delimited JSON string to a file-like object
json_buffer = io.StringIO(json_lines)

table = read_json(json_buffer)

descriptor = FlightDescriptor.for_path("my-table")
client = FlightClient("grpc://localhost:8081")

writer, _ = client.do_put(descriptor, table.schema)
writer.write_table(table)
writer.close()
