import requests

# Equivalent to curl: 
# curl -XPOST localhost:5001/query -H 'Content-Type: application/json' -d '{"table":"mytable","query":"SELECT * FROM mytable"}'

url = 'http://localhost:5001/query'
headers = {'Content-Type': 'application/json'}
data = {"table": "mytable", "query": "SELECT * FROM mytable"}

response = requests.post(url, json=data, headers=headers)

if response.status_code == 200:
    print("Response:")
    print(response.json())
else:
    print(f"Request failed with status code: {response.status_code}")
