import sys
import boto3
import datetime
from awsglue.utils import getResolvedOptions

session = boto3.Session(region_name='us-west-2') 
forecast = session.client(service_name='forecast') 
glue_client = session.client(service_name='glue')
s3 = session.resource('s3')
iam = session.resource('iam')
role = iam.Role('AmazonForecast-ExecutionRole-1569298163518')

workflowName = 'AmazonForecastWorkflow'
workflow = glue_client.get_workflow(Name=workflowName)
workflow_params = workflow['Workflow']['LastRun']['WorkflowRunProperties']
workflowRunId = workflow['Workflow']['LastRun']['WorkflowRunId']
forecastArn = workflow_params['forecastArn']
dt = datetime.datetime.now()
project = 'inventory_forecast_' + dt.strftime('%d_%m_%y')
forecastName = project + '_AutoML_forecast' 
exportJobName = forecastName + dateTime + '_export'
bucketName = 'forecast-blog-published'
s3Path = 's3://' + bucketName + '/' + forecastName

# Cleanout the older forecasts data from target bucket
bucket = s3.Bucket(bucketName)
bucket.objects.all().delete()

export_forecast_response = forecast.create_forecast_export_job(
ForecastExportJobName=exportJobName,
ForecastArn = forecastArn,
Destination={
    'S3Config': {
        'Path': s3Path,
        'RoleArn': role.arn
    }
}
)

forecastExportArn = export_forecast_response['ForecastExportJobArn']
workflow_params['forecastExportArn'] = forecastExportArn
glue_client.put_workflow_run_properties(Name=workflowName, RunId=workflowRunId, RunProperties=workflow_params)
workflow_params = glue_client.get_workflow_run_properties(Name=workflowName,
                                        RunId=workflowRunId)["RunProperties"]

print('Forecast export Arn is: ' + workflow_params['forecastExportArn'])


