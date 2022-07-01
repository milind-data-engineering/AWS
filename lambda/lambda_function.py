import json
import boto3
import os
import datetime
from datetime import datetime
from boto3.dynamodb.conditions import Key
from app.aws_eventbridge_source import func_insert_event_aws_event_source, func_modify_event_aws_event_source
#import func_insert_event_aws_event_source, func_modify_event_aws_event_source

def lambda_handler(event, context):
    
    dynamodb = boto3.resource('dynamodb')
    now = datetime.now()
    x = now.strftime("%m/%d/%Y %H:%M:%S")
    
    # Loop through messages in a SQS queue : Messages will be incoming events from object changes
    for record in event['Records']:
        
        #print(type(record["body"]))
        
        # Convert incoming payload in string to dictionary
        res = json.loads(record["body"])
        #print(type(res))
        
        # Fetch the source table name to re-direct events to proper functions
        val_source_table_name = res['detail']['eventSourceARN'].split('/')[1]
        
        #print(val_source_table_name)
        if val_source_table_name == 'aws-eventbridge-source':
            if res['detail']['eventName'] == 'INSERT':
                insert_aws_event_source = func_insert_event_aws_event_source(res,dynamodb,x)
                print(insert_aws_event_source)
            elif res['detail']['eventName'] == 'MODIFY':
                modify_aws_event_source = func_modify_event_aws_event_source(res,dynamodb,x)
        
    print('The POC worked fine!!!!')