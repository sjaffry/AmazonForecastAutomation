import boto3
import subprocess

def lambda_handler(event, context):

    session = boto3.Session(region_name='us-west-2') 
    forecast = session.client(service_name='forecast') 

    project = 'inventory_forecast'
    predictorName= project + '_AutoML'
    forecastHorizon = 24
    datasetGroupArn = event['ImportJob']['datasetGroupArn']

    create_predictor_response=forecast.create_predictor(PredictorName=predictorName, 
                                                  ForecastHorizon=forecastHorizon,
                                                  PerformAutoML= True,
                                                  PerformHPO=False,
                                                  EvaluationParameters= {"NumberOfBacktestWindows": 1, 
                                                                         "BackTestWindowOffset": 24}, 
                                                  InputDataConfig= {"DatasetGroupArn": datasetGroupArn},
                                                  FeaturizationConfig= {"ForecastFrequency": "H", 
                                                                        "Featurizations": 
                                                                        [
                                                                          {"AttributeName": "target_value", 
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
