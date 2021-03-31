import json
import requests
from datetime import datetime
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
    elastic_url ='https://search-ilab-es-hgpgwegoed6qeckoe5jnilw47u.us-west-2.es.amazonaws.com/'+site+'/_doc/'
    response = requests.post(elastic_url, data = data, auth=(es_AN,es_password), headers = headers)
    print('elasticsearch Check---------------------------')
    print (response)
