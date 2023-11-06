import json
import io
from pyarrow.flight import Action, FlightClient, Result
import pyarrow as pa

client = FlightClient("grpc://localhost:8081")
my_action_body_bytes = json.dumps({"key":"value"}).encode()
action = Action("my-action",my_action_body_bytes)
result = client.do_action(action)

for r in result:
    message = r.body.to_pybytes().decode()
    print(message)



