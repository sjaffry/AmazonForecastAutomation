import boto3
import os
import datetime

def lambda_handler(event, context):

    session = boto3.Session(region_name='us-west-2') 
    forecast = session.client(service_name='forecast') 

    dt = datetime.datetime.now()
    dateTime = dt.strftime('%Y/%m/%d')
    project = 'inventory_forecast'
    forecastName = project + '_AutoML_forecast_' + dt.strftime('%d_%m_%y')
    forecastArn = event['ForecastJobArn']
    exportJobName = forecastName + '_export'
    bucketName = os.environ['BucketName']
    filePath = exportJobName + '/' + dateTime
    s3Path = 's3://' + bucketName + '/' + filePath
    role_arn = os.environ['ForecastRoleARN']
    
    export_forecast_response = forecast.create_forecast_export_job(
    ForecastExportJobName=exportJobName,
    ForecastArn = forecastArn,
    Destination={
        'S3Config': {
            'Path': s3Path,
            'RoleArn': role_arn
        }
    }
)
    forecastExportArn = export_forecast_response['ForecastExportJobArn']

    return forecastExportArn