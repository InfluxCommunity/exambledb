import os
import pyarrow as pa
from pyarrow import flight
import pyarrow.parquet as pq
import pandas as pd
from flask import Flask, jsonify, request
import threading


app = Flask(__name__)

@app.route('/write', methods=['POST'])
def write():
    """For eachw row passed in, read the table name,
    combine the existing data (if any) with the new data,
    write back the file."""

    data = request.json

    rows = data
    for row in data:
        table_name = row["table"]
        file_path = f"{table_name}.parquet"
        print(row)
        df = pd.DataFrame([row])
        
        df = df.drop(columns=['table'])
        
        if os.path.exists(file_path):
            existing_table = pq.read_table(file_path)
            existing_df = existing_table.to_pandas()
            combined_df = pd.concat([existing_df, df], ignore_index=True)
        else:
            combined_df = df
        
        table = pa.Table.from_pandas(combined_df)
        pq.write_table(table, file_path)

    return jsonify({"message": f"{len(rows)} row written"}), 200

# Simple Flight server implementation
class SimpleFlightServer(flight.FlightServerBase):
    def list_flights(self, context, criteria):
        # Placeholder logic
        return []

    def get_flight_info(self, context, descriptor):
        table_name = descriptor.path[0].decode("utf-8")
        file_path = f"{table_name}.parquet"
        if os.path.exists(file_path):
            table = pq.read_table(file_path)
            schema = table.schema
            endpoints = [flight.FlightEndpoint(ticket=flight.Ticket(table_name.encode('utf-8')), locations=[flight.Location.for_grpc_tcp("localhost", 8081)])]
            return flight.FlightInfo(schema, descriptor, endpoints, table.num_rows, 0)
        raise flight.FlightUnavailableError(f"No data for descriptor {table_name}")

    def do_get(self, context, ticket):
        table_name = ticket.ticket.decode('utf-8')
        file_path = f"{table_name}.parquet"
        if os.path.exists(file_path):
            table = pq.read_table(file_path)
            return flight.RecordBatchStream(table)
        raise flight.FlightUnavailableError(f"No data for ticket {table_name}")


def run_web_server():
    print("Starting Flask server on localhost:5000")
    app.run(port=5001,host="0.0.0.0")

def run_flight_server():
    location = flight.Location.for_grpc_tcp("localhost", 8081)
    server = SimpleFlightServer(location)

    print("Starting Flight server on localhost:8081")
    server.serve()

if __name__ == "__main__":
    t1 = threading.Thread(target=run_web_server)
    t2 = threading.Thread(target=run_flight_server)

    t1.start()
    t2.start()

    t1.join()
    t2.join()
