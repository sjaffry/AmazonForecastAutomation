import boto3

def lambda_handler(event, context):

    session = boto3.Session(region_name='us-west-2') 
    forecast = session.client(service_name='forecast') 

    ds_import_job_arn = event['ImportJob']['importJobArn']
    dataImportStatus = forecast.describe_dataset_import_job(DatasetImportJobArn=ds_import_job_arn)['Status']
    
    return dataImportStatus