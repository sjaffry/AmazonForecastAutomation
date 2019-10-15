import boto3
import os

def lambda_handler(event, context):

    session = boto3.Session(region_name='us-west-2') 
    glue = session.client(service_name='glue') 

    crawlerName = os.environ['Crawler_Name']
   
    response = glue.start_crawler(Name=crawlerName)

    return response