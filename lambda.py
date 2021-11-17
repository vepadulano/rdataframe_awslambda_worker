import threading
import base64
import json
import os
import time
import cloudpickle as pickle
from ast import literal_eval

import boto3
import ROOT

bucket = os.getenv('bucket')
monitor = (os.getenv('monitor', 'False') == 'True')

results = []


def monitor_me():
    inspector = Inspector()
    inspector.inspectAll()
    inspector.inspectAllDeltas()
    return inspector.finish()


class MonitoringThread(threading.Thread):
    """Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition."""

    def __init__(self, *args, **kwargs):
        super(MonitoringThread, self).__init__(*args, **kwargs)
        self._stop_event = threading.Event()
        self._results = []

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

    def run(self):
        while not self.stopped():
            results.append(monitor_me())
            time.sleep(1)


def lambda_handler(event, context):
    if monitor:
        thread = MonitoringThread()
        thread.start()

    print('event', event)
    s3 = boto3.client('s3')

    start = int(event['start'])
    end = int(event['end'])

    range = base64.b64decode(event['range'][2:-1])
    mapper = base64.b64decode(event['script'][2:-1])
    cert_file = base64.b64decode(event['cert'][2:-1])

    mapper = pickle.loads(mapper)
    range = pickle.loads(range)

    with open("/tmp/certs", "wb") as handle:
        pickle.dump(cert_file, handle)

    try:
        hist = mapper(range)
    except Exception as e:
        return {
            'statusCode': 500,
            'errorType': json.dumps(type(e).__name__),
            'errorMessage': json.dumps(str(e)),
        }

    with open('/tmp/out.pickle', 'wb') as handle:
        pickle.dump(hist, handle)

    filename = f'partial_{str(start)}_{str(end)}_{str(int(time.time()*1000.0))}.pickle'
    s3.upload_file(f'/tmp/out.pickle', bucket, filename)

    if monitor:
        thread.stop()
        thread.join()

    return {
        'statusCode': 200,
        'body': json.dumps(results),
        'filename': json.dumps(filename)
    }
