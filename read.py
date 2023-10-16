import pyarrow as pa
from pyarrow import flight


# Create a client connecting to our Flight server
client = flight.FlightClient("grpc://localhost:8081")

# Descriptor for the Flight stream
descriptor = flight.FlightDescriptor.for_path("mytable")

# Request flight info (which contains the ticket to get the actual data)
flight_info = client.get_flight_info(descriptor)

# Use the first endpoint's ticket to fetch the actual data
# In this simple example, we assume there's only one endpoint.
reader = client.do_get(flight_info.endpoints[0].ticket)

# Read the stream into an Arrow table
table = reader.read_all()

print(table.to_pandas())


