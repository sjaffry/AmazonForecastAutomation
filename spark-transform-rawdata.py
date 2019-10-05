import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
LANDING_BUCKET = "forecast-blog-landing"
PROCESSED_BUCKET = "forecast-blog-processed"

orders = glueContext.create_dynamic_frame_from_options("s3", connection_options={"paths": ["s3://"+LANDING_BUCKET+"/orders"]}, format="csv")
orders1 = orders.select_fields(["col0", "col2", "col3", "col5"], transformation_ctx="", info="", stageThreshold=0, totalThreshold=0)

products = glueContext.create_dynamic_frame_from_options("s3", connection_options={"paths": ["s3://"+LANDING_BUCKET+"/products"]}, format="csv")
products1 = products.drop_fields(["col2", "col6", "col8", "col9"], transformation_ctx="", info="", stageThreshold=0, totalThreshold=0)

resolvechoice1 = ResolveChoice.apply(frame = orders1, choice = "make_struct", transformation_ctx = "resolvechoice1")
resolvechoice2 = ResolveChoice.apply(frame = products1, choice = "make_struct", transformation_ctx = "resolvechoice2")

dropnullfields_orders = DropNullFields.apply(frame = resolvechoice1, transformation_ctx = "dropnullfields_orders")
dropnullfields_products = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields_products")

# We want a single combined file for loading into Amazon forecast
orders_combined = dropnullfields_orders.repartition(1)
products_combined = dropnullfields_products.repartition(1)

datasink = glueContext.write_dynamic_frame.from_options(frame = orders_combined, connection_type = "s3", connection_options = {"path": "s3://"+PROCESSED_BUCKET+"/orders/raw"}, format = "csv", format_options={"writeHeader": False}, transformation_ctx = "datasink")
datasink2 = glueContext.write_dynamic_frame.from_options(frame = products_combined, connection_type = "s3", connection_options = {"path": "s3://"+PROCESSED_BUCKET+"/products/raw"}, format = "csv", format_options={"writeHeader": False}, transformation_ctx = "datasink2")


# Let's copy the orders file and rename it so that it can be loaded into Amazon Forecast
# and we maintain only a single file each time
client = boto3.client('s3')
response1 = client.list_objects(
    Bucket=PROCESSED_BUCKET,
    Prefix="orders/raw"
)

ordersfile = response1["Contents"][0]["Key"]
print(ordersfile)

client.copy_object(Bucket=PROCESSED_BUCKET, CopySource=PROCESSED_BUCKET+"/"+ordersfile, Key="orders/orders-data.csv")

client.delete_object(Bucket=PROCESSED_BUCKET, Key=ordersfile)

# Let's copy the products file and rename it so that it can be loaded into Amazon Forecast
# and we maintain only a single file each time
response2 = client.list_objects(
    Bucket=PROCESSED_BUCKET,
    Prefix="products/raw"
)

productsfile = response2["Contents"][0]["Key"]
print(productsfile)

client.copy_object(Bucket=PROCESSED_BUCKET, CopySource=PROCESSED_BUCKET+"/"+productsfile, Key="products/product-data.csv")

client.delete_object(Bucket=PROCESSED_BUCKET, Key=productsfile)

job.commit()