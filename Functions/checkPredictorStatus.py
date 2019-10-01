import boto3

def lambda_handler(event, context):

    session = boto3.Session(region_name='us-west-2') 
    forecast = session.client(service_name='forecast') 

    predictorArn = event['PredictorJobArn']
    predictorStatus = forecast.describe_predictor(PredictorArn=predictorArn)['Status']
    
    return predictorStatus