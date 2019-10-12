import boto3

def lambda_handler(event, context):

    session = boto3.Session(region_name='us-west-2') 
    forecast = session.client(service_name='forecast') 

    # Get the orders data import status
    orders_import_job_arn = event['ImportJob']['importJobArnOrders']
    OrdersDataImportStatus = forecast.describe_dataset_import_job(DatasetImportJobArn=orders_import_job_arn)['Status']

    # Get the products data import status
    products_import_job_arn = event['ImportJob']['importJobArnProducts']
    productsDataImportStatus = forecast.describe_dataset_import_job(DatasetImportJobArn=products_import_job_arn)['Status']    

    if (OrdersDataImportStatus == 'ACTIVE' and productsDataImportStatus == 'ACTIVE'):
        dataImportStatus = 'ACTIVE'
    elif (OrdersDataImportStatus == 'CREATE_FAILED' or productsDataImportStatus == 'CREATE_FAILED'):
       dataImportStatus = 'CREATE_FAILED' 
    else:
        dataImportStatus = 'CREATE_PENDING'

    return dataImportStatus