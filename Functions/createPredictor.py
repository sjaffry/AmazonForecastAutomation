import boto3
import subprocess
import datetime

def lambda_handler(event, context):

    session = boto3.Session(region_name='us-west-2') 
    forecast = session.client(service_name='forecast') 

    dt = datetime.datetime.now()
    project = 'inventory_forecast_' + dt.strftime('%d_%m_%y')
    predictorName= project + '_AutoML'
    forecastHorizon = 2
    datasetGroupArn = event['ImportJob']['datasetGroupArn']

    create_predictor_response=forecast.create_predictor(PredictorName=predictorName, 
                                                  ForecastHorizon=forecastHorizon,
                                                  PerformAutoML= True,
                                                  PerformHPO=False,
                                                  EvaluationParameters= {"NumberOfBacktestWindows": 1, 
                                                                         "BackTestWindowOffset": 2}, 
                                                  InputDataConfig= {"DatasetGroupArn": datasetGroupArn},
                                                  FeaturizationConfig= {"ForecastFrequency": "15min", 
                                                                        "Featurizations": 
                                                                        [
                                                                          {"AttributeName": "demand", 
                                                                           "FeaturizationPipeline": 
                                                                            [
                                                                              {"FeaturizationMethodName": "filling", 
                                                                               "FeaturizationMethodParameters": 
                                                                                {"frontfill": "none", 
                                                                                 "middlefill": "zero", 
                                                                                 "backfill": "zero"}
                                                                              }
                                                                            ]
                                                                          }
                                                                        ]
                                                                       }
                                                        )
    predictorArn=create_predictor_response['PredictorArn']
    
    print ('Predictor Arn: ' + predictorArn)

    return predictorArn
