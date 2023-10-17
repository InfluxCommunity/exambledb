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
    data = request.json
    table_name = data["table"]
    rows = data["rows"]

    df = pd.DataFrame(rows)

    file_path = f"{table_name}.parquet"

    # Initialize combined_df with the new data
    combined_df = df

    # add the new data to the old data, if any old data
    if os.path.exists(file_path):
        existing_table = pq.read_table(file_path)
        existing_df = existing_table.to_pandas()

        # Check if there's a primary key specified in the data
        if "primary_key" in data:
            primary_key = data["primary_key"]
            
            for row in rows:
                # Create a boolean mask for matching rows based on the keys and their values
                mask = existing_df.all(axis=1)
                for key in primary_key:
                    mask &= (existing_df[key] == row[key])

                existing_records = existing_df[mask]

                if not existing_records.empty:
                    # Update the existing records with new values
                    for index, record in existing_records.iterrows():
                        for key, value in row.items():
                            existing_df.at[index, key] = value
                else:
                    # Append the new data to the existing data
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
        else:
            # Append the new data to the existing data without checks
            combined_df = pd.concat([existing_df, df], ignore_index=True)
    
    # write the data to the parquet file
    table = pa.Table.from_pandas(combined_df)
    pq.write_table(table, file_path)

    return jsonify({"message": f"{len(rows)} rows written"}), 204

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
    app.run(port=5001, host="0.0.0.0")

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

