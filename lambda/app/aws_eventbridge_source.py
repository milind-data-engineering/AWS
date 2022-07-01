import os
from boto3.dynamodb.conditions import Key

def func_insert_event_aws_event_source(res,dynamodb,x):
    table = dynamodb.Table('aws-eventbridge-scd2')
            
    # Fetch New Image Values
    val = res['detail']['dynamodb']['NewImage']['ID']['N']
    val_accountid = res['detail']['dynamodb']['NewImage']['ACCOUNT_ID']['S']
    val_managing_split_percent = res['detail']['dynamodb']['NewImage']['MANAGING_SPLIT_PERCENT']['N']

    print(val)
    print(type(val))
    table.put_item(Item={'ID': int(val), 'ACCOUNT_ID':val_accountid, 'MANAGING_SPLIT_PERCENT':val_managing_split_percent,'IS_ACTIVE': 'Y', 'START_DATETIMESTAMP': x, 'END_DATETIMESTAMP':'12/31/9999 00:00:00'})
    print('Record inserted into DDB')
            
def func_modify_event_aws_event_source(res,dynamodb,x):
    
    print('Update loop reached')
    table = dynamodb.Table('aws-eventbridge-scd2')
        
    # Get the key name 
    k = '%s' %os.environ.get('Exception')
    print(k)
        
    # Fetch New Image Values
    val = res['detail']['dynamodb']['NewImage']['ID']['N']
    val_accountid = res['detail']['dynamodb']['NewImage']['ACCOUNT_ID']['S']
    val_managing_split_percent = res['detail']['dynamodb']['NewImage']['MANAGING_SPLIT_PERCENT']['N']
            
    # Fetch Old Image values. Do not move the below 3 lines outside of the Modify event
    val_new = res['detail']['dynamodb']['OldImage']['ID']['N']
    val_accountid_old = res['detail']['dynamodb']['OldImage']['ACCOUNT_ID']['S']
    val_managing_split_percent_old = res['detail']['dynamodb']['OldImage']['MANAGING_SPLIT_PERCENT']['N']
            
    # Update the existing record in target table & set the IS_ACTIVE flag to N, END_DATE to current date. 
    #Issue encountered: In DDB, can't update the sort-key of End Date within update_item
    #table.update_item(Key= {'ID':int(val), 'END_DATETIMESTAMP': '12/31/9999 00:00:00'}, UpdateExpression="SET IS_ACTIVE=:s, ACCOUNT_ID=:t, MANAGING_SPLIT_PERCENT=:u",ExpressionAttributeValues={':s':'N',':t':val_accountid_old,':u':val_managing_split_percent_old}, ReturnValues="UPDATED_NEW")
    #print('Existing Record Updated')
            
    # Fetch the START_DATETIMESTAMP of the key to be deleted
    response_start_time = table.query(ProjectionExpression="START_DATETIMESTAMP", KeyConditionExpression=(Key('ID').eq(int(val)) & Key('END_DATETIMESTAMP').eq('12/31/9999 00:00:00')))
    print(response_start_time['Items'][0]['START_DATETIMESTAMP'])
            
    # Remove the existing record which has an END_DATE of 12/31/9999 00:00:00
    table.delete_item(Key= {'ID':int(val), 'END_DATETIMESTAMP': '12/31/9999 00:00:00'})
            
    # Re-Insert the old record with active flag as N: Refer OldImage
    table.put_item(Item={'ID': int(val), 'ACCOUNT_ID':val_accountid_old, 'MANAGING_SPLIT_PERCENT':val_managing_split_percent_old,'IS_ACTIVE': 'N', 'START_DATETIMESTAMP': response_start_time['Items'][0]['START_DATETIMESTAMP'], 'END_DATETIMESTAMP': x})
            
    # Insert the new record with active flag as Y : Refer NewImage
    table.put_item(Item={'ID': int(val), 'ACCOUNT_ID':val_accountid, 'MANAGING_SPLIT_PERCENT':val_managing_split_percent,'IS_ACTIVE': 'Y', 'START_DATETIMESTAMP': x, 'END_DATETIMESTAMP':'12/31/9999 00:00:00'})
    print('Record inserted into DDB')





