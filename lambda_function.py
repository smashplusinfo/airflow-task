import json
import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):
	bucket ='airflow-dag-task'

	custAddress = {}
	custAddress['name'] = 'Joe Dann'
	custAddress['Street'] = 'Down Town'
	custAddress['City'] = 'Alaska'
	custAddress['customerId'] = 'CID-11111'

	fileName = 'CusJoe697' + '.json'

	uploadByteStream = bytes(json.dumps(custAddress).encode('UTF-8'))

	s3.put_object(Bucket=bucket, Key=fileName, Body=uploadByteStream)

	print('Added Completec')
