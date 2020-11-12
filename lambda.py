import json
import boto3
import botocore
import os
import io
import urllib3
import zipfile

def lambda_handler(event, context):
    print("started")
    s3 = boto3.client('s3')
    print("downloading obj")
    s3.download_file(event['in_bucket_name'], event['file_path'], f'/tmp/{event["file_path"]}')
    print("streamed obj")

    script_file = open('/tmp/script.py', "a")
    script_file.write(event['script'])
    script_file.close()
     
        
    result=os.system('''
        export PATH=/mnt/cern_root/chroot/usr/local/sbin:/mnt/cern_root/chroot/usr/local/bin:/mnt/cern_root/chroot/usr/sbin:/mnt/cern_root/chroot/usr/bin:/mnt/cern_root/chroot/sbin:/mnt/cern_root/chroot/bin:$PATH && \
        export LD_LIBRARY_PATH=/mnt/cern_root/chroot/usr/lib64:/mnt/cern_root/chroot/usr/lib:/usr/lib64:/usr/lib:$LD_LIBRARY_PATH && \
        export roothome=/mnt/cern_root/root_install && \
        chmod 777 /mnt/cern_root/chroot/usr/bin/python3.7 && \
        chmod 777 /mnt/cern_root/root_install/bin/root-config && \
        . ${roothome}/bin/thisroot.sh && \
        python3 /tmp/script.py
    ''')
    
    s3.upload_file(f'/tmp/out.root', event['out_bucket_name'], event['file_name'])

    return {
        'statusCode': 200,
        'body': json.dumps('Extracted ROOT to EFS!'),
        'result': json.dumps(result)
    }
