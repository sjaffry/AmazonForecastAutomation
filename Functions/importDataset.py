import boto3
import os
import datetime

session = boto3.Session(region_name='us-west-2') 
forecast = session.client(service_name='forecast') 

# In our dataset, the timeseries values are recorded every minute
DATASET_FREQUENCY = "1min" 
TIMESTAMP_FORMAT = "yyyy-MM-dd hh:mm:ss"

def lambda_handler(event, context):
    
    dt = datetime.datetime.now()
    project = 'inventory_forecast_' + dt.strftime('%d_%m_%y') 
    datasetName = project + '_ds'
    datasetGroupName = project + '_dsg'
    bucket_name = os.environ['S3BucketName']
    orders_file = os.environ['OrdersFile']
    products_file = os.environ['ProductsFile']
    role_arn = os.environ['ForecastRoleARN']
    s3DataPathOrders = 's3://' + bucket_name + '/' + orders_file
    s3DataPathProducts = 's3://' + bucket_name + '/' + products_file

    create_dataset_group_response = forecast.create_dataset_group(DatasetGroupName=datasetGroupName,
                                                              Domain="INVENTORY_PLANNING",
                                                             )
    datasetGroupArn = create_dataset_group_response['DatasetGroupArn']

    orders_import_result = start_orders_import_job(s3DataPathOrders, datasetName, datasetGroupArn, role_arn)
    products_import_result = start_products_import_job(s3DataPathProducts, datasetName, datasetGroupArn, role_arn, orders_import_result['ordersDatasetArn'])

    return {
        "importJobArnOrders": orders_import_result['importJobArn'],
        "importJobArnProducts": products_import_result['importJobArn'],
        "datasetGroupArn": datasetGroupArn
    }

def start_orders_import_job(s3DataPath, datasetName, datasetGroupArn, role_arn):

    # Specify the schema of your dataset here. Make sure the order of columns matches the raw data files.
    schema = {
	"Attributes": [
		{
			"AttributeName": "timestamp",
			"AttributeType": "timestamp"
		},
		{
			"AttributeName": "item_id",
			"AttributeType": "string"
		},
		{
			"AttributeName": "demand",
			"AttributeType": "integer"
		},
		{
			"AttributeName": "location",
			"AttributeType": "string"
		}
	]
    }

    response = forecast.create_dataset(
                    Domain="INVENTORY_PLANNING",
                    DatasetType='TARGET_TIME_SERIES',
                    DatasetName=datasetName,
                    DataFrequency=DATASET_FREQUENCY, 
                    Schema = schema)

    datasetArn = response['DatasetArn']

    updateDatasetResponse = forecast.update_dataset_group(DatasetGroupArn=datasetGroupArn, DatasetArns=[datasetArn])

    # Dataset import job
    datasetImportJobName = 'INVENTORY_DSIMPORT_JOB_TARGET'
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
        "datasetGroupArn": datasetGroupArn,
        "ordersDatasetArn": datasetArn
    }

def start_products_import_job(s3DataPath, datasetName, datasetGroupArn, role_arn, ordersDatasetArn):

    # Specify the schema of your dataset here. Make sure the order of columns matches the raw data files.
    schema = {
        "Attributes": [
            {
                "AttributeName": "item_id",
                "AttributeType": "string"
            },
            {
                "AttributeName": "description",
                "AttributeType": "string"
            },
            {
                "AttributeName": "price",
                "AttributeType": "float"
            }
        ]
    }

    response = forecast.create_dataset(
                    Domain="INVENTORY_PLANNING",
                    DatasetType='ITEM_METADATA',
                    DatasetName=datasetName+'_2',
                    DataFrequency=DATASET_FREQUENCY, 
                    Schema = schema)

    datasetArn = response['DatasetArn']

    updateDatasetResponse = forecast.update_dataset_group(DatasetGroupArn=datasetGroupArn, DatasetArns=[ordersDatasetArn, datasetArn])

    # Dataset import job
    datasetImportJobName = 'INVENTORY_DSIMPORT_JOB_METADATA'
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
    print('Metadata Dataset import job ARN: ' + ds_import_job_arn)
    
    return {
        "importJobArn": ds_import_job_arn,
        "datasetGroupArn": datasetGroupArn
    }