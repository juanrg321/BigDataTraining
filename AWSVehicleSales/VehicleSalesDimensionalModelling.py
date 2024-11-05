import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from awsglue.dynamicframe import DynamicFrame
import boto3

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
s3_client = boto3.client('s3')
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket_name_to_delete = 'myvehiclesalesproject'
folder_path_to_delete = 'stagingsilver/toprocess/'
folder_path_destination = 'stagingsilver/processed/'

def write_to_redshift(dynamic_frame, table_name):
    # Use the Glue connection name instead of individual parameters
    redshift_connection_name = "Redshift connection"  # Replace with your actual Glue connection name
    
    glueContext.write_dynamic_frame.from_options(
        dynamic_frame,
        connection_type="redshift",
        connection_options={
            "url": f"jdbc:redshift://workgroup3.145023101378.us-east-1.redshift-serverless.amazonaws.com:5439/dev",
            "dbtable": f"public.{table_name}",  # Dynamic table name
            "redshiftTmpDir": "s3://myvehiclesalesproject/temp/",  # Specify your temporary S3 bucket
            "aws_iam_role": "arn:aws:iam::145023101378:role/pleaseworkrole",  # If using IAM role
            "user": "admin",  # Your Redshift username
            "password": "Password1"
        }
    )

def move_s3_folder(bucket, source_prefix, destination_prefix):
    # List objects in the source folder
    objects_to_move = s3_client.list_objects_v2(Bucket=bucket, Prefix=source_prefix)

    if 'Contents' in objects_to_move:
        for obj in objects_to_move['Contents']:
            old_key = obj['Key']
            new_key = old_key.replace(source_prefix, destination_prefix, 1)

            # Copy the object to the new location
            s3_client.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': old_key},
                Key=new_key
            )
            print(f"Copied {old_key} to {new_key}")

            # Delete the original object
            s3_client.delete_object(Bucket=bucket, Key=old_key)
            print(f"Deleted {old_key}")
    else:
        print(f"No objects found in {source_prefix}")
        
# Define schemas
schema = StructType([
    StructField("year", IntegerType(), True),
    StructField("make", StringType(), True),
    StructField("model", StringType(), True),
    StructField("trim", StringType(), True),
    StructField("body", StringType(), True),
    StructField("transmission", StringType(), True),
    StructField("vin", StringType(), True),
    StructField("state", StringType(), True),
    StructField("condition", DoubleType(), True),
    StructField("odometer", DoubleType(), True),
    StructField("color", StringType(), True),
    StructField("interior", StringType(), True),
    StructField("seller", StringType(), True),
    StructField("mmr", DoubleType(), True),
    StructField("sellingprice", DoubleType(), True),
    StructField("saledate", StringType(), True)
])

dim_date_schema = StructType([
    StructField("year", IntegerType(), True),
    StructField("month", StringType(), True),
    StructField("day", StringType(), True),
    StructField("quarter", StringType(), True),
    StructField("saledate", StringType(), True),
    StructField("date_id", StringType(), True)
])

vehicle_schema = StructType([
    StructField("vin", StringType(), True),
    StructField("make", StringType(), True),
    StructField("model", StringType(), True),
    StructField("trim", StringType(), True),
    StructField("body", StringType(), True),
    StructField("transmission", StringType(), True),
    StructField("color", StringType(), True),
    StructField("interior", StringType(), True),
    StructField("year", IntegerType(), True)
])

seller_schema = StructType([
    StructField("seller", StringType(), True),
    StructField("state", StringType(), True),
    StructField("seller_id", StringType(), True)
])

# Read input data
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://myvehiclesalesproject/stagingsilver/toprocess/"]},
    format="parquet"
)
df = dynamic_frame.toDF()

# Generate distinct IDs
# Create dimension tables
dim_date_df = df.select(
    F.year("saledate").alias("year"),
    F.date_format("saledate", 'MMMM').alias("month"),
    F.dayofmonth("saledate").alias("day"),
    F.quarter("saledate").alias("quarter"),
    "saledate",
    F.dense_rank().over(Window.orderBy("saledate")).alias("date_id").cast(IntegerType())
).distinct()

# Generate seller_id for the seller dimension
dim_seller_df = df.select("seller", "state").distinct() \
    .withColumn("seller_id", F.dense_rank().over(Window.orderBy("seller")).cast(IntegerType()))  # Adding seller_id

dim_vehicle_df = df.select("vin", "make", "model", "trim", "body", "transmission", "color", "interior", "year").distinct()

# Prepare the fact table
fact_table_df = df.select(
    "vin",
    F.dense_rank().over(Window.orderBy("seller")).alias("seller_id").cast(IntegerType()), 
    "sellingprice",
    "saledate",
    "odometer",
    "condition",
    F.dense_rank().over(Window.orderBy("saledate")).alias("date_id").cast(IntegerType())
)
# Output paths
hdfs_path_output_sellers = "s3://myvehiclesalesproject/curatedgold/dim_sellers/"
hdfs_path_output_date = "s3://myvehiclesalesproject/curatedgold/dim_date/"
hdfs_path_output_vehicles = "s3://myvehiclesalesproject/curatedgold/dim_vehicles/"
hdfs_path_output_fact_table = "s3://myvehiclesalesproject/curatedgold/fact_table/"

# Write dimension tables
cleaned_dim_date_dyf = DynamicFrame.fromDF(dim_date_df, glueContext, "cleaned_dim_date_dyf")

# Write back as DynamicFrame
glueContext.write_dynamic_frame.from_options(cleaned_dim_date_dyf, connection_type="s3", connection_options={"path": hdfs_path_output_date}, format="parquet")

cleaned_dim_seller_dyf = DynamicFrame.fromDF(dim_seller_df, glueContext, "cleaned_dim_seller_dyf")

# Write back as DynamicFrame
glueContext.write_dynamic_frame.from_options(cleaned_dim_seller_dyf, connection_type="s3", connection_options={"path": hdfs_path_output_sellers}, format="parquet")

cleaned_dim_vehicle_dyf = DynamicFrame.fromDF(dim_vehicle_df, glueContext, "cleaned_dim_vehicle_dyf")

# Write back as DynamicFrame
glueContext.write_dynamic_frame.from_options(cleaned_dim_vehicle_dyf, connection_type="s3", connection_options={"path": hdfs_path_output_vehicles}, format="parquet")


# Write fact table
cleaned_dim_fact_dyf = DynamicFrame.fromDF(fact_table_df, glueContext, "cleaned_dim_fact_dyf")

# Write back as DynamicFrame
glueContext.write_dynamic_frame.from_options(cleaned_dim_fact_dyf, connection_type="s3", connection_options={"path": hdfs_path_output_fact_table}, format="parquet")

"""write_to_redshift(cleaned_dim_date_dyf, "dim_date")
write_to_redshift(cleaned_dim_seller_dyf, "dim_sellers")
write_to_redshift(cleaned_dim_vehicle_dyf, "dim_vehicles")
write_to_redshift(cleaned_dim_fact_dyf, "fact_table")"""
move_s3_folder(bucket_name_to_delete, folder_path_to_delete, folder_path_destination)
job.commit()