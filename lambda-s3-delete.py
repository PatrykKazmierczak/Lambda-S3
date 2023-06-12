from __future__ import print_function
import boto3
import urllib.parse

s3 = boto3.client('s3')

def lambda_handler(event, context):
    print("*" * 80)
    print("Initializing..")
    print("*" * 80)

    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        object_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])

        print("Using waiter to wait for object to persist through S3 service")
        waiter = s3.get_waiter('object_exists')
        waiter.wait(Bucket=bucket, Key=object_key)

        response = s3.head_object(Bucket=bucket, Key=object_key)
        print("CONTENT TYPE: " + str(response['ContentType']))
        print('ETag: ' + str(response['ETag']))
        print('Content-Length: ' + str(response['ContentLength']))
        print('KeyName: ' + str(object_key))
        print('Deleting object: ' + str(object_key))

        s3.delete_object(Bucket=bucket, Key=object_key)

        return {
            'statusCode': 200,
            'body': 'Success'
        }
    except Exception as err:
        print("Error -", str(err))
        return {
            'statusCode': 500,
            'body': 'Error'
        }
