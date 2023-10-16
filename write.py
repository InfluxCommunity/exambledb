import requests

data = [{
    "table":"garystable",
    "dog": "andy",
    "weight": 2
}, {
    "table":"garystable",
    "dog": "oscar",
    "weight": 3
},{
    "table":"garystable",
    "dog": "red",
    "weight": 4
} ]

response = requests.post("http://localhost:5001/write", json=data)

print(response.status_code)
print(response.text)
