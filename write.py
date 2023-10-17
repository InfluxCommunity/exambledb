import requests

data = [{
    "table":"mytable",
    "col1": "two",
    "col2": 2,
    "primary_key": ["col1"]
}, {
    "table":"mytable",
    "col1": "three",
    "col2": 3,
    "primary_key": ["col1"]
},{
    "table":"mytable",
    "col1": "four",
    "col2": 4,
    "primary_key": ["col1"]
} ]

response = requests.post("http://localhost:5001/write", json=data)

print(response.status_code)
print(response.text)
