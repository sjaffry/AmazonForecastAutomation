import sys
import boto3
import pyspark.sql.functions as F
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
## @params: [JOB_NAME]
LANDING_BUCKET = "forecast-blog-landing"
PROCESSED_BUCKET = "forecast-blog-processed"

orders = glueContext.create_dynamic_frame_from_options("s3", connection_options={"paths": ["s3://"+LANDING_BUCKET+"/orders"]}, format="csv", format_options={"withHeader": True}, transformation_ctx = "orders")

ordersDF = orders.toDF()

ordersDF1 = ordersDF.select(F.to_timestamp("InvoiceDate",'MM/dd/yy HH:mm').alias('timestamp'), "StockCode", "Quantity", "Country")

ordersDF2 = ordersDF1.withColumnRenamed("StockCode","item_id").withColumnRenamed("Quantity","demand").withColumnRenamed("Country","location")

ordersDF3 = ordersDF2.withColumn('timestamp',F.from_unixtime(F.unix_timestamp('timestamp', 'yyyy-MM-dd HH:mm:ss.SSS'),'yyyy-MM-dd HH:mm:ss'))

ordersDF4 = ordersDF3.repartition(1)

ordersDF4.write.csv("s3://forecast-blog-processed/orders/raw")

productsDF1 = ordersDF.select("StockCode", "Description", "UnitPrice")

productsDF2 = productsDF1.withColumnRenamed("StockCode","item_id")

productsDF3 = productsDF2.repartition(1)

productsDF3.write.csv("s3://forecast-blog-processed/products/raw")

client = boto3.client('s3')

response = client.list_objects(
    Bucket=PROCESSED_BUCKET,
    Prefix="orders/raw"
)

ordersfile = response["Contents"][0]["Key"]
print(ordersfile)

client.copy_object(Bucket=PROCESSED_BUCKET, CopySource=PROCESSED_BUCKET+"/"+ordersfile, Key="orders/orders-data.csv")

client.delete_object(Bucket=PROCESSED_BUCKET, Key=ordersfile)

response = client.list_objects(
    Bucket=PROCESSED_BUCKET,
    Prefix="products/raw"
)

productsfile = response["Contents"][0]["Key"]
print(productsfile)

client.copy_object(Bucket=PROCESSED_BUCKET, CopySource=PROCESSED_BUCKET+"/"+productsfile, Key="products/product-data.csv")

client.delete_object(Bucket=PROCESSED_BUCKET, Key=productsfile)