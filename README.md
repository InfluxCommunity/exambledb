<img src="https://github.com/lmangani/simpledb/assets/1423657/9e2d6647-d92b-4549-8f7a-7a842983d74a" width=150 />

# SimpleDB

> A python example for creating your own online database using [Apache Arrow](https://arrow.apache.org/).

<br>

#### :rainbow: About

This _'hello world'_ example is not intended to be _complete, feature-rich or highly efficient_<br>
It was designed to inspire developers to utilize [Apache Arrow](https://arrow.apache.org/) for _creating their own databases_.


##### Building Blocks

- [Flask server](https://flask.palletsprojects.com/) with `/write` and `/query` endpoints
- [Flight server](https://arrow.apache.org/blog/2022/02/16/introducing-arrow-flight-sql/) for sending queries
- [Datafusion](https://arrow.apache.org/datafusion/user-guide/introduction.html) for process sql queries
- [Parquet](https://arrow.apache.org/docs/python/parquet.html) file storage

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
