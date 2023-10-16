import requests

data = {
    "table":"mytable",
    "rows":[
        {"col1":"one",
         "col2":1},
        {"col1":"two",
         "col2":2}         
    ]}

response = requests.post("http://localhost:5001/write", json=data)

print(response.status_code)
print(response.text)
