import pyarrow as pa
from pyarrow import flight
from pyarrow.flight import FlightClient, Ticket, FlightCallOptions
import json

client = flight.FlightClient("grpc://localhost:8081")
ticket_bytes = json.dumps({'sql':'select * from mytable', 'table':'mytable'})
ticket = Ticket(ticket_bytes)
reader = client.do_get(ticket)
print(reader.read_all().to_pandas())


