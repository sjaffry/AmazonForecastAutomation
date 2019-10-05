import boto3
import os
import datetime

def lambda_handler(event, context):
    
    session = boto3.Session(region_name='us-west-2') 
    forecast = session.client(service_name='forecast') 

    # In our dataset, the timeseries valuesa are recorded hourly
    DATASET_FREQUENCY = "H" 
    TIMESTAMP_FORMAT = "yyyy-MM-dd hh:mm:ss"

    dt = datetime.datetime.now()
    project = 'inventory_forecast_' + dt.strftime('%d_%m_%y') 
    datasetName = project + '_ds'
    datasetGroupName = project + '_dsg'
    bucket_name = os.environ['S3BucketName']
    orders_file = os.environ['Orders']
    products_file = os.environ['Products']
    s3DataPath = 's3://' + bucket_name + '/' + orders_file

    create_dataset_group_response = forecast.create_dataset_group(DatasetGroupName=datasetGroupName,
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

    response = forecast.create_dataset(
                    Domain="CUSTOM",
                    DatasetType='TARGET_TIME_SERIES',
                    DatasetName=datasetName,
                    DataFrequency=DATASET_FREQUENCY, 
                    Schema = schema)

    datasetArn = response['DatasetArn']

    updateDatasetResponse = forecast.update_dataset_group(DatasetGroupArn=datasetGroupArn, DatasetArns=[datasetArn])

    role_arn = os.environ['ForecastRoleARN']

    # Dataset import job
    datasetImportJobName = 'EP_DSIMPORT_JOB_TARGET'
    ds_import_job_response=forecast.create_dataset_import_job(DatasetImportJobName=datasetImportJobName,
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
    print('Dataset import job ARN: ' + ds_import_job_arn)

    return {
        "importJobArn": ds_import_job_arn,
        "datasetGroupArn": datasetGroupArn
    }

    







