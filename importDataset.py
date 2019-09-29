import boto3
import json
import os

def lambda_handler(event, context):
    
    #bucket_name = os.environ['S3BucketName']
    bucket_name = event['Bucketname']
    #session = boto3.Session(region_name='us-west-2') 
    client = boto3.client('forecast')
    #forecastquery = session.client(service_name='forecastquery')

    # In our dataset, the timeseries valuesa are recorded hourly
    DATASET_FREQUENCY = "H" 
    TIMESTAMP_FORMAT = "yyyy-MM-dd hh:mm:ss"

    project = 'inventory_forecast'
    datasetName = project+'_ds'
    datasetGroupName = project+'_dsg'
    #fileName = os.environ['TrainingDataFile']
    fileName = event['Filename']
    s3DataPath = "s3://"+bucket_name+"/"+fileName

    create_dataset_group_response = client.create_dataset_group(DatasetGroupName=datasetGroupName,
                                                              Domain="CUSTOM",
                                                             )
    datasetGroupArn = create_dataset_group_response['DatasetGroupArn']

    # Specify the schema of your dataset here. Make sure the order of columns matches the raw data files.
    schema = {
    "Attributes":[
        {
            "AttributeName":"timestamp",
            "AttributeType":"timestamp"
        },
        {
            "AttributeName":"target_value",
            "AttributeType":"float"
        },
        {
            "AttributeName":"item_id",
            "AttributeType":"string"
        }
    ]
    }

    response = client.create_dataset(
                    Domain="CUSTOM",
                    DatasetType='TARGET_TIME_SERIES',
                    DatasetName=datasetName,
                    DataFrequency=DATASET_FREQUENCY, 
                    Schema = schema)

    datasetArn = response['DatasetArn']

    updateDatasetResponse = client.update_dataset_group(DatasetGroupArn=datasetGroupArn, DatasetArns=[datasetArn])
    print(updateDatasetResponse)

    role_arn = os.environ['ForecastRoleARN']
    print(role_arn)

    # Dataset import job
    datasetImportJobName = 'EP_DSIMPORT_JOB_TARGET'
    ds_import_job_response=client.create_dataset_import_job(DatasetImportJobName=datasetImportJobName,
                                                          DatasetArn=datasetArn,
                                                          DataSource= {
                                                              "S3Config" : {
                                                                 "Path": s3DataPath,
                                                                 "RoleArn": role_arn
                                                              } 
                                                          },
                                                          TimestampFormat=TIMESTAMP_FORMAT
                                                         )

    ds_import_job_arn=ds_import_job_response['DatasetImportJobArn']
    print("Dataset import job AR: "+ds_import_job_arn)

    return ds_import_job_arn

    







