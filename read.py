import pyarrow as pa
from pyarrow import flight

client = flight.FlightClient("grpc://localhost:8081")
descriptor = flight.FlightDescriptor.for_path("select col1 from mytable", "mytable")
flight_info = client.get_flight_info(descriptor)
reader = client.do_get(flight_info.endpoints[0].ticket)
table = reader.read_all()

print(table.to_pandas())


