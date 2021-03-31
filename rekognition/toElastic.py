import json
import requests
from datetime import datetime
import config

def storeToElastic(data, site):
    # data = event
    # eventTimestamp = int(data['frame']["captureResult"]['timestamp'])
    # eventTimestamp = datetime.fromtimestamp(eventTimestamp)
    # #data['eventTimestamp'] = datetime.fromtimestamp(float(data['eventTimestamp']))
    
    # nowString = eventTimestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
    # data['eventTimestamp'] = nowString

    data = json.dumps(data)
    headers={'Accept': 'application/json', 'Content-type': 'application/json'}
    elastic_url =config.es_url+site+'/_doc/'
    response = requests.post(config.es_url, data = data, auth=(config.es_AN,config.es_password), headers = headers)
    print('elasticsearch Check---------------------------')
    print (response)
