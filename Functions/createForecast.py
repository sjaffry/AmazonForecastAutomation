import boto3

def lambda_handler(event, context):

    session = boto3.Session(region_name='us-west-2') 
    forecast = session.client(service_name='forecast') 

    project = 'inventory_forecast'
    forecastName= project + '_AutoML_forecast'
    predictorArn = event['PredictorJobArn']
    
    create_forecast_response=forecast.create_forecast(ForecastName=forecastName,
                                                  PredictorArn=predictorArn)
    forecastArn = create_forecast_response['ForecastArn']

    return forecastArn