import base64
import json
import os
import time
import cloudpickle as pickle
from ast import literal_eval

import boto3
import ROOT

bucket = os.environ.get('bucket')

def lambda_handler(event, context):
    print('event', event)
    s3 = boto3.client('s3')

    start = int(event['start'])
    end = int(event['end'])

    range = base64.b64decode(event['range'][2:-1])
    mapper = base64.b64decode(event['script'][2:-1])
    cert_file =  base64.b64decode(event['cert'][2:-1])

    mapper=pickle.loads(mapper)
    range=pickle.loads(range)

    with open("/tmp/certs", "wb") as handle:
        pickle.dump(cert_file, handle)

    try:
        hist=mapper(range)
    except Exception as e:
        return {
            'statusCode': 500,
            'errorType': json.dumps(type(e).__name__),
            'errorMessage': json.dumps(str(e)),
        }

    # f = ROOT.TFile('/tmp/out.root', 'RECREATE')
    # for h in hist:
    #     h.GetValue().Write()
    # f.Close()
    
    with open('/tmp/out.pickle', 'wb') as handle:
        pickle.dump(hist, handle)

    filename=f'partial_{str(start)}_{str(end)}_{str(int(time.time()*1000.0))}.pickle'

    s3.upload_file(f'/tmp/out.pickle', bucket, filename)

    return {
        'statusCode': 200,
        'body': json.dumps(f'Done analyzing, result saved as {filename}')
    }
    