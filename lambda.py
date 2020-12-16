import base64
import json
import os

import boto3

bucket = os.environ.get('bucket')


def lambda_handler(event, context):
    print('event', event)
    s3 = boto3.client('s3')

    start = str(event['start'])
    end = str(event['end'])

    range = base64.b64decode(event['range'][2:-1])
    mapper = base64.b64decode(event['script'][2:-1])
    print("streamed obj")

    glue = f"""
import pickle
mapper=pickle.loads({mapper})
range=pickle.loads({range})
hist=mapper(range)
pickle.dump(hist, open('/tmp/out.pickle','w'))
"""

    script_file = open('/tmp/to_execute.py', "w")
    script_file.write(glue)
    script_file.close()

    result = os.system('''
        export PATH=/mnt/cern_root/chroot/usr/local/sbin:/mnt/cern_root/chroot/usr/local/bin:/mnt/cern_root/chroot/usr/sbin:/mnt/cern_root/chroot/usr/bin:/mnt/cern_root/chroot/sbin:/mnt/cern_root/chroot/bin:$PATH && \
        export LD_LIBRARY_PATH=/mnt/cern_root/chroot/usr/lib64:/mnt/cern_root/chroot/usr/lib:/usr/lib64:/usr/lib:$LD_LIBRARY_PATH && \
        export CPATH=/mnt/cern_root/chroot/usr/include:$CPATH && \
        export PYTHONPATH=/mnt/cern_root/root_install/PyRDF:/mnt/cern_root/root_install:$PYTHONPATH && \
        export roothome=/mnt/cern_root/root_install && \
        cd /mnt/cern_root/root_install/PyRDF && \
        . ${roothome}/bin/thisroot.sh && \
        /mnt/cern_root/chroot/usr/bin/python3.7 -m pip install cloudpickle numpy --target=/mnt/cern_root/root_install && \
        /mnt/cern_root/chroot/usr/bin/python3.7 /tmp/to_execute.py
    ''')
    if not result:
        return {
            'statusCode': 500,
            'body': json.dumps('Failed!'),
            'result': json.dumps(result)
        }

    s3.upload_file(f'/tmp/out.pickle', bucket, f'partial_{start}_{end}.pickle')

    return {
        'statusCode': 200,
        'body': json.dumps('Extracted ROOT to EFS!'),
        'result': json.dumps(result)
    }
