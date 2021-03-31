import json
import requests
from datetime import datetime
es_url = ""
es_AN = ""
es_password = ""
def storeToElastic(data, site):
    # data = event
    # eventTimestamp = int(data['frame']["captureResult"]['timestamp'])
    # eventTimestamp = datetime.fromtimestamp(eventTimestamp)
    # #data['eventTimestamp'] = datetime.fromtimestamp(float(data['eventTimestamp']))
    
    # nowString = eventTimestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
    # data['eventTimestamp'] = nowString

    data = json.dumps(data)
    headers={'Accept': 'application/json', 'Content-type': 'application/json'}
    elastic_url =es_url+site+'/_doc/'
    response = requests.post(elastic_url, data = data, auth=(es_AN,es_password), headers = headers)
    print('elasticsearch Check---------------------------')
    print (response)
