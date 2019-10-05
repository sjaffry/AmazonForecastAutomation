import boto3

def lambda_handler(event, context):

    session = boto3.Session(region_name='us-west-2') 
    glue = session.client(service_name='glue') 

    jobId = event['JobRunId']
    jobRunId = jobId['JobRunId']
    jobStatus = glue.get_job_run(JobName='forecast-blog-transform-data',RunId=jobRunId)
    
    return jobStatus['JobRun']['JobRunState']