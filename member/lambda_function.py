import boto3
from datetime import datetime
import time
import json
import os
import io
import config
import api



def lambda_handler(event,context):
    s3_client = boto3.client('s3', 
                    aws_access_key_id=config.AWS_ACCESS_KEY,
                    aws_secret_access_key=config.AWS_SECRET_KEY)
    csv_obj = s3_client.get_object(Bucket = config.AWS_S3_BUCKET,Key = config.AWS_S3_FILE_KEY)
    body = csv_obj['Body']
    json_string = body.read().decode('utf-8')
    staffRecord = json.loads(json_string)
    body.close()

    frame = {
        "frameId" : event['frameId'],
        "timestamp" : event['eventTimestamp'],
        "imageUrl" : event['imageUrl'],
        "site" : event['site']
    }
    
    recordList = []

    eventList = api.getData(event)
        
    for member in config.memberList:
        flag = 0
        for i in eventList:
            if member in i:
                flag = 1
        if flag ==1:
            continue
        
        print(member)

        if  member not in staffRecord:
            continue
        judge = staffRecord[member]

        # member last Record In not trigger this function

            

            
        eventProfile = {
                "frameId" : judge['frameId'],
                "eventTimestamp" : judge['eventTimestamp'],
                "name" : member,
                "frameUrl":judge['frameUrl'],
                "site" :"OUT"
            }
        staffRecord[eventProfile['name']] = eventProfile
        api.writeData(staffRecord)

        frame2 = {
                "frameId" :  judge['frameId'],
                "timestamp" :judge['eventTimestamp'],
                "imageUrl" : judge['frameUrl'],
                "site" : "OUT"
            }

        behaviorDetection = {
                "personId" : member,
                "inTime" : 0,
                "outTime" : judge['eventTimestamp'],
                "isMember" :1,
                "stayTime" : judge['eventTimestamp']- event['eventTimestamp'],
                "coordinate_x" : 0.0,
                "coordinate_y" : 0.0
            }

            
        fraudModel = {
                "frame" :[frame2],
                "behaviorDetection" : behaviorDetection
            }
        recordList.append(fraudModel)

    if len(eventList) !=0:
        print(len(eventList))
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
                api.writeData(staffRecord)
                continue 
            recordProfile = staffRecord[eventProfile['name']]
            print(recordProfile)

            frame2 = {
                        "frameId" :  recordProfile['frameId'],
                        "timestamp" :recordProfile['eventTimestamp'],
                        "imageUrl" : recordProfile['frameUrl'],
                        "site" : recordProfile['site']
                    }
            
                

            if eventProfile['site'] == 'IN':
                    # last Time  is "OUT"
                    # stay Time = in -out
                    #[frame,frame2] = [IN,OUT]
                    

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
                api.writeData(staffRecord)
                recordList.append(fraudModel)



    return recordList
