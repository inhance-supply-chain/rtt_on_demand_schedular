import requests
import json


#  http://apiondemand.rtt.co.za

base_url = 'http://192.168.1.20:5000/on_demand/run_solver'
dict_headers = {"date":'2021-04-26', "cluster_id":json.dumps([1, 2, 3])}
username = 'rtt_on_demand'
password = 'bPgwQj2VyVMn5xIj'

payload = requests.get(base_url, auth=(username, password), params=dict_headers)


import json
test = [1, 2]
test = json.dumps(test)
print(test)