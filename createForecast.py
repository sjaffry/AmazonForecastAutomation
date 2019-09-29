import boto3
import subprocess

def lambda_handler(event, context):

    session = boto3.Session(region_name='us-west-2') 
    forecast = session.client(service_name='forecast') 
    forecastquery = session.client(service_name='forecastquery')

    forecastName= project+'_AutoML_forecast'
    PredictorArn = event['PredictorArn']
    
    create_forecast_response=forecast.create_forecast(ForecastName=forecastName,
                                                  PredictorArn=predictorArn)
    forecastArn = create_forecast_response['ForecastArn']

    return forecastArn