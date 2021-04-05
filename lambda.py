import base64
import json
import os
import time
import cloudpickle as pickle
from ast import literal_eval

import boto3

bucket = os.environ.get('bucket')

def lambda_handler(event, context):
    print('event', event)
    s3 = boto3.client('s3')

    start = int(event['start'])
    end = int(event['end'])
    filelist= literal_eval(event['filelist'])
    friend_info= None
    
    if event.get('friend_info'):
        friend_info = pickle.loads(
            base64.b64decode(event['friend_info'][2:-1])
        )

    range = base64.b64decode(event['range'][2:-1])
    mapper = base64.b64decode(event['script'][2:-1])

    mapper=pickle.loads(mapper)
    range=pickle.loads(range)

    range.start=start
    range.end=end
    range.filelist=filelist
    print("before friend")
    if friend_info is not None:
        print(friend_info)
        print(friend_info.friend_names)
        print(friend_info.friend_file_names)

    range.friend_info=friend_info
    print("after friend")

    hist=mapper(range)
    print("after map")
    pickle.dump(hist, open('/tmp/out.pickle','wb'))

    filename=f'partial_{str(start)}_{str(end)}_{str(int(time.time()*1000.0))}.pickle'

    s3.upload_file(f'/tmp/out.pickle', bucket, filename)

    return {
        'statusCode': 200,
        'body': json.dumps(f'Done analyzing, result saved as {filename}')
    }
