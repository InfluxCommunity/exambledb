import os
import pyarrow as pa
from pyarrow import flight
import pyarrow.parquet as pq
import datafusion
from datafusion import SessionContext
import pandas as pd
from flask import Flask, jsonify, request
import threading


app = Flask(__name__)

@app.route('/write', methods=['POST'])
def write():
    data = request.json

    for row in data:
        table_name = row["table"]
        file_path = f"{table_name}.parquet"
        print(row)
        df = pd.DataFrame([row])
        
        df = df.drop(columns=['table'])
        
        if os.path.exists(file_path):
            existing_table = pq.read_table(file_path)
            existing_df = existing_table.to_pandas()
            
            # Check if there's a primary key specified in the row
            if "primary_key" in row:
                primary_key = row["primary_key"]
                
                # Create a boolean mask for matching rows based on the keys
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
                    existing_df = pd.concat([existing_df, df], ignore_index=True)
            else:
                # Append the new data to the existing data if no primary key is specified
                existing_df = pd.concat([existing_df, df], ignore_index=True)
        else:
            existing_df = df
        
        table = pa.Table.from_pandas(existing_df)
        pq.write_table(table, file_path)

    return jsonify({"message": f"{len(data)} rows written or updated"}), 200

# Simple Flight server implementation
class SimpleFlightServer(flight.FlightServerBase):
    def list_flights(self, context, criteria):
        # Placeholder logic
        return []

    def get_flight_info(self, context, descriptor):
        # Extract SQL query from the descriptor's path
        sql_query = descriptor.path[0].decode()
        table_name = descriptor.path[1].decode()
        ticket = flight.Ticket(sql_query + ":" + table_name)  # encode SQL query in the ticket
        endpoints = [flight.FlightEndpoint(ticket=ticket, locations=[flight.Location.for_grpc_tcp("localhost", 8081)])]
        schema = pq.read_schema(f"{table_name}.parquet")  # you can derive schema directly from the parquet file
        return flight.FlightInfo(schema, descriptor, endpoints, total_records=-1, total_bytes=-1)
    
    def do_get(self, context, ticket):
        try:
            sql_query, table_name = ticket.ticket.decode().split(":")
            
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
