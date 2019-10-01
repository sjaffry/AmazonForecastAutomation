import boto3

def lambda_handler(event, context):

    session = boto3.Session(region_name='us-west-2') 
    forecast = session.client(service_name='forecast') 

    forecastArn = event['ForecastJobArn']
    forecastStatus = forecast.describe_forecast(ForecastArn=forecastArn)['Status']
    
    return forecastStatus