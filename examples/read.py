#!/usr/bin/env python3

from pyarrow.flight import Ticket, FlightClient
import json

client = FlightClient("grpc://localhost:8081")
ticket_bytes = json.dumps({'sql':'select * from mytable', 'table':'mytable'})
ticket = Ticket(ticket_bytes)
reader = client.do_get(ticket)
print(reader.read_all().to_pandas())
