<img src="https://github.com/lmangani/simpledb/assets/1423657/9e2d6647-d92b-4549-8f7a-7a842983d74a" width=150 />

# SimpleDB

> A python example for creating your own online database using Apache Arrow.

<br>

#### :rainbow: About

This _"hello world"_ example is not meant to be efficient for featureful and was designed <br>
to inspire developers to use the Apache Arrow project to write their own datatbases.


##### Building Blocks

-  Flask server with `/write` and `/query` endpoints
- Flight server for sending queries
- Datafusion for process sql queries
- All data in stored in `parquet` files

<br>

### :zap: Usage
#### Requirements
```
pip install -r requirements.txt
```

#### :bulb: Server
Start SimpleDB server on `localhost:8081`
```
./simpledb.py
```

#### :bulb: Client
Test your SimpleDB service using the included `examples`

##### :round_pushpin: Write _(REST)_
```
./examples/write.py
```
##### :round_pushpin: Read _(REST)_
```
./examples/read_rest.py
```
##### :round_pushpin: Read _(GRPC)_
```
./examples/read.py
```
