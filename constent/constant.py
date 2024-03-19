import boto3

dynamodb_table_name = "video_processing_tracker"
s3 = boto3.client('s3', region_name='us-east-1')
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')