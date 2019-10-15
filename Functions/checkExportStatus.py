import boto3

def lambda_handler(event, context):

    session = boto3.Session(region_name='us-west-2') 
    forecast = session.client(service_name='forecast') 

    forecastExportArn = event['ForecastExportArn']
    forecastExportStatus = forecast.describe_forecast_export_job(ForecastExportJobArn=forecastExportArn)['Status']
    
    return forecastExportStatus