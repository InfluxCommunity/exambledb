import os
import pyarrow as pa
from pyarrow import flight
import pyarrow.parquet as pq
from datafusion import SessionContext
import pandas as pd
from flask import Flask, jsonify, request
import threading
import json

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

    return jsonify({"message": f"{len(rows)} rows written"}), 200

# Simple Flight server implementation
class SimpleFlightServer(flight.FlightServerBase):
    def list_flights(self, context, criteria):
        # Placeholder logic
        return []

    def get_flight_info(self, context, descriptor):
        return None
    
    def do_get(self, context, ticket):
        try:
            ticket_obj = json.loads(ticket.ticket.decode())
            sql_query = ticket_obj["sql"]
            table_name = ticket_obj["table"]
            
            # Using DataFusion to execute the SQL query
            ctx = SessionContext()
            ctx.register_parquet(table_name, f"{table_name}.parquet")
            
            result = ctx.sql(sql_query)
            table = result.to_arrow_table()
            
            return flight.RecordBatchStream(table)
        except Exception as e:
            print(e)


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
