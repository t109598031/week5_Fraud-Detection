import requests
import pandas as pd
import numpy as np
from datetime import datetime,timedelta
import time
import re
import json
import numpy as np
import boto3
import io

es_url = ""
es_AN = ""
es_password = ""
es_index = "IN"
AWS_S3_BUCKET = ""
AWS_S3_FILE = ""
AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""
s3_client = boto3.client(
    "s3",
    aws_access_key_id = AWS_ACCESS_KEY,
    aws_secret_access_key = AWS_SECRET_KEY,
    region_name = "us-west-2"
    )


def elasticsearchQueryGetMatchAll(es_url,index_name,doc_num,num,es_AN,es_password):
    query = json.dumps({
  "sort" :[{"eventTimestamp":{"order":"asc"}}],
  "query" : {
    "match_all": {}
  }
})
    headers={'Accept': 'application/json','Content-type': 'application/json'}
    response = requests.get("{es_url}/{index_name}/_search/?size={doc_num}&from={num}".format(es_url = es_url,
                                                                                            index_name = index_name,
                                                                                            doc_num = doc_num,
                                                                                            num = num),
                        data=query, 
                        auth=(es_AN,es_url),
                        headers = headers)
    result = json.loads(response.text)
    return result['hits']['hits']

def getData(es_url,index_name,es_AN,es_password):
    response = requests.get("{es_url}/_cat/count/{index_name}".format(es_url = es_url,index_name = index_name),auth=(es_AN,es_password))
    docCount = int(re.split('\s',response.text)[2])
    data = []
    start = 0
    for doc in range(int(docCount/50)+1):
        data += elasticsearchQueryGetMatchAll(es_url,index_name,50,start)
        start+=50
    return data 

def staffDataTransform(event,staffRecord):
    frame = {
        "frameId" : event['frameId'],
        "timestamp" : event['eventTimestamp'],
        "imageUrl" : event['imageUrl'],
        "site" : event['site']
    }
    recordList = []
    eventList = []
    faceCount = len(event['searchFaceResponse'])
    for i in range(faceCount):
        if len(event['searchFaceResponse'][i]['FaceMatches'])>0:
            eventTimestamp = event['eventTimestamp']
            eventList.append([event['frameId'],
                              eventTimestamp,event['searchFaceResponse'][i]['FaceMatches'][0]['Face']['ExternalImageId']
                              ,event['imageUrl'],
                              event['site']])
 
    if len(eventList) !=0:

        for count in range(len(eventList)):            
            eventProfile = {
                "frameId" : eventList[count][0],
                "eventTimestamp" : eventList[count][1],
                "name" : eventList[count][2],
                "frameUrl":eventList[count][3],
                "site" :eventList[count][4]
            }
            print(eventProfile)

            if eventProfile['name'] not in staffRecord:
                staffRecord[eventProfile['name']] = eventProfile
                continue 
            recordProfile = staffRecord[eventProfile['name']]


            frame2 = {
                        "frameId" :  recordProfile['frameId'],
                        "timestamp" :recordProfile['eventTimestamp'],
                        "imageUrl" : recordProfile['frameUrl'],
                        "site" : recordProfile['site']
                    }
            
            if eventProfile['site'] == 'IN':
                behaviorDetection = {
                    "personId" : eventProfile['name'],
                    "inTime" : eventProfile['eventTimestamp'],
                    "outTime" : recordProfile['eventTimestamp'],
                    "isMember" :1,
                    "stayTime" : recordProfile['eventTimestamp'] - eventProfile['eventTimestamp'],
                    "coordinate_x" : 0.0,
                    "coordinate_y" : 0.0
                    }

                fraudModel = {
                    "frame" :[frame,frame2],
                    "behaviorDetection" : behaviorDetection
                    }
                staffRecord[eventProfile['name']] = eventProfile
                recordList.append(fraudModel)
    return recordList,staffRecord
def notMemberDataTransfrom(event,notMember):
    if event["site"]!="IN":
        return 

    outputModel
        
    currentNotMemberCount = 0
    currentNotMemberList = []
    for person in event["ppeDetectResponse"]["Persons"]:
        for bodypart in person["BodyParts"]:
            if (bodypart["Name"] == "HEAD") and (len(bodypart["EquipmentDetections"]) != 0):
                currentNotMemberCount = currentNotMemberCount + 1
                currentNotMemberList.append({
                        "X":bodypart["EquipmentDetections"][0]["BoundingBox"]["Left"]+0.5*bodypart["EquipmentDetections"][0]["BoundingBox"]["Width"],
                        "Y":bodypart["EquipmentDetections"][0]["BoundingBox"]["Top"]+0.5*bodypart["EquipmentDetections"][0]["BoundingBox"]["Height"],
                        "matched": False
                    })
                break
                
    preNotMemberList = notMember
    preNotMemberCount = len(preNotMemberList)
        
    for notMember in preNotMemberList:
        notMember["matched"] = False
        notMember["crossLine"] = 0
            
        #################
    print("acurrentNotMemberList",currentNotMemberList)
    print("apreNotMemberList",preNotMemberList)
        
        
    if preNotMemberCount >= currentNotMemberCount :
        for currentNotMember in currentNotMemberList:
            nearestPreNotMemberIndex = -1
            nearestPreNotMemberDistance = 100
            for index in range(preNotMemberCount):
                if preNotMemberList[index]["matched"] == False:
                    distanceX = preNotMemberList[index]["coordinate"]["X"]-currentNotMember["X"]
                    distanceY = preNotMemberList[index]["coordinate"]["Y"]-currentNotMember["Y"]
                    distance = math.sqrt(distanceX ** 2 + distanceY ** 2)
                    if distance < nearestPreNotMemberDistance:
                        nearestPreNotMemberDistance = distance
                        nearestPreNotMemberIndex = index
                            
            if currentNotMember["X"] <0.5 and preNotMemberList[nearestPreNotMemberIndex]["coordinate"]["X"]>=0.5:
                preNotMemberList[nearestPreNotMemberIndex]["crossLine"] = 1
            elif currentNotMember["X"] >0.5 and preNotMemberList[nearestPreNotMemberIndex]["coordinate"]["X"]<=0.5:
                preNotMemberList[nearestPreNotMemberIndex]["crossLine"] = 2
            preNotMemberList[nearestPreNotMemberIndex]["coordinate"]["X"] = currentNotMember["X"]
            preNotMemberList[nearestPreNotMemberIndex]["coordinate"]["Y"] = currentNotMember["Y"]
            preNotMemberList[nearestPreNotMemberIndex]["matched"] = True
            preNotMemberList[nearestPreNotMemberIndex]["missingTime"] = 0
            print("preNotMemberList",preNotMemberList)   
                
    elif currentNotMemberCount > preNotMemberCount :
        for preNotMember in preNotMemberList:
            nearestCurrentNotMemberIndex = -1
            nearestCurrentNotMemberDistance = 100
            print("bcurrentNotMemberList",currentNotMemberList)
            print("bpreNotMemberList",preNotMemberList)  
            for index in range(currentNotMemberCount):
                if currentNotMemberList[index]["matched"] == False:
                    distanceX = currentNotMemberList[index]["X"]-preNotMember["coordinate"]["X"]
                    distanceY = currentNotMemberList[index]["Y"]-preNotMember["coordinate"]["Y"]
                    distance = math.sqrt(distanceX ** 2 + distanceY ** 2)
                    if distance<nearestCurrentNotMemberDistance:
                        nearestCurrentNotMemberDistance = distance
                        nearestCurrentNotMemberIndex = index
                        
            print("ccurrentNotMemberList",currentNotMemberList)
            print("cpreNotMemberList",preNotMemberList)     
            if currentNotMemberList[nearestCurrentNotMemberIndex]["X"] <0.5 and preNotMember["coordinate"]["X"]>=0.5:
                preNotMember["crossLine"] = 1
            elif currentNotMemberList[nearestCurrentNotMemberIndex]["X"] >0.5 and preNotMember["coordinate"]["X"]<=0.5:
                preNotMember["crossLine"] = 2
            preNotMember["coordinate"]["X"] = currentNotMemberList[nearestCurrentNotMemberIndex]["X"]
            preNotMember["coordinate"]["Y"] = currentNotMemberList[nearestCurrentNotMemberIndex]["Y"]
            preNotMember["matched"] = True
            preNotMember["missingTime"] = 0
            currentNotMemberList[nearestCurrentNotMemberIndex]["matched"] = True
        print("dcurrentNotMemberList",currentNotMemberList)
        print("dpreNotMemberList",preNotMemberList) 
        notMemberId = 1
        for currentNotMember in currentNotMemberList:
            if currentNotMember["matched"] == False:
                preNotMemberList.append({
                        "notMemberId": "nonMember_"+str(event["eventTimestamp"])+"_"+str(notMemberId),
                        "entryTime": event["eventTimestamp"],
                        "missingTime": 0,
                        "coordinate":{
                            "X":currentNotMember["X"],
                            "Y":currentNotMember["Y"]
                        },
                        "matched": True,
                        "crossLine":0
                    })
                preNotMemberCount = preNotMemberCount +1
            notMemberId = notMemberId+1
            
    storeNotMemberList = []
    for index in range(preNotMemberCount):
        if preNotMemberList[index]["matched"] == False:
            preNotMemberList[index]["missingTime"] = preNotMemberList[index]["missingTime"] + 1
            
        if preNotMemberList[index]["missingTime"] < 2:
            storeNotMemberList.append(preNotMemberList[index])
            outputModel.append({
                    "frame":[{
                        "frameId": event["frameId"],
                        "timestamp": event["eventTimestamp"],
                        "imageUrl": event["imageUrl"],
                        "site": event["site"]
                    }],
                    "behaviorDetection":{
                        "personId": preNotMemberList[index]["notMemberId"],
                        "isMember": 0,
                        "inTime": preNotMemberList[index]["entryTime"],
                        "outTime": 0,
                        "stayTime": event["eventTimestamp"] - preNotMemberList[index]["entryTime"],
                        "coordinate_x": preNotMemberList[index]["coordinate"]["X"],
                        "coordinate_y": preNotMemberList[index]["coordinate"]["Y"],
                        "crossLine":preNotMemberList[index]["crossLine"]
                    }
                })
            
            
    return outputModel,storeNotMemberList


INData = getData(es_url,"IN",es_AN,es_password)
staff = {}
notMember = []
fraud_data = []

for i in range(len(INData)):
    event = INData[i]['_source']
    fraudModelList,staffRecord =  staffDataTransform(event,staffReocrd)
    for fraudModel in fraudModelList:
        if fraudModel['stayTime'] < -120:
            record = [fraudModel['eventTimestamp'],fraudModel['personId'],fraudModel['stayTime'],1]
        else:
            record = [fraudModel['eventTimestamp'],fraudModel['personId'],fraudModel['stayTime'],0]
        fraud_data.append(record)

for i in range(len(INData)):
    event = INData[i]['_source']
    fraudModelList,staffRecord =  staffDataTransform(event,notMember)
    for fraudModel in fraudModelList:
        if fraudModel['stayTime'] >120:
            record = [fraudModel['eventTimestamp'],fraudModel['personId'],fraudModel['stayTime'],1]
        else:
            record = [fraudModel['eventTimestamp'],fraudModel['personId'],fraudModel['stayTime'],0]
        fraud_data.append(record)

fraud_data_pd = pd.DataFrame(fraud_data)
fraud_data_pd.columns = ['EVENT_TIMESTAMP','person_id','stay_time','EVENT_LABEL']

with io.StringIO() as csv_buffer:
    fraud_data_pd.to_csv(csv_buffer, index=False,header=None)

    response = s3_client.put_object(
        Bucket=AWS_S3_BUCKET, Key=AWS_S3_FILE, Body=csv_buffer.getvalue(),ACL='public-read',ContentType = 'text/csv'
    )
