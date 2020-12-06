import json
import os

import boto3


def lambda_handler(event, context):
    print("started")
    s3 = boto3.client('s3')
    print("downloading obj")
    s3.download_file(event['in_bucket_name'], event['file_path'], '/tmp/in.root')
    print("streamed obj")

    intro = """
import sys
print(sys.argv)
fileName = sys.argv[0]
treeName = sys.argv[1]
"""

    outro = """
outHistFile = ROOT.TFile.Open("out.root","RECREATE")
outHistFile.cd()
hist.write()
outHistFile.Close()
"""

    script_file = open('/tmp/script.py', "a")
    script_file.write(intro)
    script_file.write(event['script'])
    script_file.write(outro)
    script_file.close()

    result = os.system('''
        export PATH=/mnt/cern_root/chroot/usr/local/sbin:/mnt/cern_root/chroot/usr/local/bin:/mnt/cern_root/chroot/usr/sbin:/mnt/cern_root/chroot/usr/bin:/mnt/cern_root/chroot/sbin:/mnt/cern_root/chroot/bin:$PATH && \
        export LD_LIBRARY_PATH=/mnt/cern_root/chroot/usr/lib64:/mnt/cern_root/chroot/usr/lib:/usr/lib64:/usr/lib:$LD_LIBRARY_PATH && \
        export CPATH=/mnt/cern_root/chroot/usr/include:$CPATH && \      
        export roothome=/mnt/cern_root/root_install && \
        export PYTHONPATH=/mnt/cern_root/root_install/PyRDF:$PYTHONPATH && \     
        . ${roothome}/bin/thisroot.sh && \
        cd /tmp && \
        python3.7 /tmp/script.py /tmp/in.root myTree
    ''')
    if not result:
        return {
            'statusCode': 500,
            'body': json.dumps('Failed!'),
            'result': json.dumps(result)
        }

    s3.upload_file(f'/tmp/out.root', event['out_bucket_name'], event['file_path'])

    return {
        'statusCode': 200,
        'body': json.dumps('Extracted ROOT to EFS!'),
        'result': json.dumps(result)
    }
