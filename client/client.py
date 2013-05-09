import json
from pprint import pprint
json_data=open('data.log')

data = json.load(json_data)
pprint(data)
json_data.close()
