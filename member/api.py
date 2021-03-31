import config
import boto3
import io
import json

s3_client = boto3.client('s3', 
                    aws_access_key_id=config.AWS_ACCESS_KEY,
                    aws_secret_access_key=config.AWS_SECRET_KEY)


def getData(event):
    eventList = []
    # print(event[0])
    faceCount = len(event['searchFaceResponse'])
    for i in range(faceCount):
        if len(event['searchFaceResponse'][i]['FaceMatches'])>0:
            eventTimestamp = event['eventTimestamp']
            eventList.append([event['frameId'],eventTimestamp,event['searchFaceResponse'][i]['FaceMatches'][0]['Face']['FaceId'],event['imageUrl'],event['site']])
    return eventList



def writeData(jsonFile):   
    s3_client.put_object(Body=str(json.dumps(jsonFile)),Bucket = config.AWS_S3_BUCKET,Key = config.AWS_S3_FILE_KEY)









