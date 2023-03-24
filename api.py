import json
import pandas as pd
import boto3

def read_s3_file_into_df(s3, s3_bucket_mame, file):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=s3_bucket_mame, Key=file)
    df = pd.read_csv(obj['Body'])
    return df

##########################
# S3 Varaibles
s3 = boto3.resource('s3')
s3_client = boto3.client("s3")

s3_subslist_master_bucket = 'kala.dev.subs-list-master'
filename_subslist = 'MasterData.csv'

s3_subslist_error_bucket = 'kala.dev.alert-log'
filename_error = 'Alert.csv'
##########################

def lambda_handler(event, context):
    # Get the HTTP method and path from the event
    http_method = event['httpMethod']
    path = event['path']
    
    # Define a dictionary with the endpoints and their corresponding handler functions
    endpoints = {
        'GET': {
            '/errorqueue': get_errors,
            '/notification/{priority}': get_notifications,
        }
        # ,
        # 'POST': {
        #    '/transactions':
        # },
        # 'PUT': {
        #     '/transaction/{invoice_number}': update_user
        # }
    }
    
    # Check if the endpoint and HTTP method are valid
    if http_method in endpoints and path in endpoints[http_method]:
        # Call the corresponding handler function
        handler = endpoints[http_method][path]
        return handler(event, context)
    
    # Handle invalid requests
    else:
        response = {
            'statusCode': 404,
            'body': json.dumps({'message': 'Resource not found'})
        }
        return response


def get_errors(event, context):
    # Get a list of errors and return a response
    data=read_s3_file_into_df(s3, s3_subslist_error_bucket, filename_error)

    errors = []
    
    for d in range(len(data.to_dict()[list(data.to_dict().keys())[0]])):
        error={
            'error_name':data.to_dict()['error_name'][d],
            'related_invoice':data.to_dict()['related_invoice'][d],
            'error_details':data.to_dict()['error_details'][d],
            'priority':data.to_dict()['priority'][d],
            'status':data.to_dict()['status'][d]
        }
        errors.append(error)
    
    response = {
        'statusCode': 200,
        'body': json.dumps(errors)
    }
    return response


def get_notifications(event, context):
    # Get a list of notifications and return a response
    data=read_s3_file_into_df(s3, s3_subslist_error_bucket, filename_error)

    notifications = []
    
    for d in range(len(data.to_dict()[list(data.to_dict().keys())[0]])):
        priority = event['pathParameters']['priority']
        if str(data.to_dict()['priority'][d])==str(priority):
            notification={
                'notification_name':data.to_dict()['notification_name'][d],
                'related_invoice':data.to_dict()['related_invoice'][d],
                'notification_details':data.to_dict()['notification_details'][d],
                'priority':data.to_dict()['priority'][d],
                'status':data.to_dict()['status'][d]
            }
            notifications.append(notification)
    
    response = {
        'statusCode': 200,
        'body': json.dumps(notifications)
    }
    return response