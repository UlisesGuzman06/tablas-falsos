import urllib.request
import json

url = "http://127.0.0.1:5000/detalles"
payload = {"dia": "3", "columna": "Oct-25_false"}

try:
    req = urllib.request.Request(url)
    req.add_header('Content-Type', 'application/json; charset=utf-8')
    jsondata = json.dumps(payload).encode('utf-8')
    req.add_header('Content-Length', len(jsondata))
    
    response = urllib.request.urlopen(req, jsondata)
    print(f"Status: {response.getcode()}")
    print(f"Response: {response.read().decode('utf-8')}")
except urllib.error.HTTPError as e:
    print(f"HTTPError: {e.code}")
    print(f"Error Content: {e.read().decode('utf-8')}")
except Exception as e:
    print(f"Error: {e}")
