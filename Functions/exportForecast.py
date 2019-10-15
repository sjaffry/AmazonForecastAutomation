import boto3
import os
import datetime

def lambda_handler(event, context):

    session = boto3.Session(region_name='us-west-2') 
    forecast = session.client(service_name='forecast') 
    s3 = boto3.resource('s3')

    dt = datetime.datetime.now()
    dateTime = dt.strftime('%Y%m%d')
    project = 'inventory_forecast'
    forecastName = project + '_AutoML_forecast'
    forecastArn = event['ForecastJobArn']
    exportJobName = forecastName + dateTime + '_export'
    bucketName = os.environ['BucketName']
    s3Path = 's3://' + bucketName + '/' + forecastName
    role_arn = os.environ['ForecastRoleARN']
    
    # Cleanout the older forecasts data from target bucket
    bucket = s3.Bucket(bucketName)
    bucket.objects.all().delete()

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