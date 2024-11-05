import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, TimestampType
from awsglue.dynamicframe import DynamicFrame

def cleaning_data(df_vehicle_sales):
    null_values_dropped_df = df_vehicle_sales.na.drop()

    trimmed_df = null_values_dropped_df.select(
        [F.trim(F.col(c)).alias(c) for c in null_values_dropped_df.columns if c != "saledate"] + [F.col("saledate")]
    )

    to_lower_df = trimmed_df.select(
    [F.lower(F.col(c)).alias(c) for c in trimmed_df.columns if c not in ["saledate", "vin"]] + [F.col("saledate"), F.col("vin")])


    remove_duplicates_df = to_lower_df.dropDuplicates(["vin"])

    remove_outliers_df1 = remove_duplicates_df.filter(
        ~((F.col("make") == "ford") & (F.col("model") == "escape") & (F.col("sellingprice") > 150000))
    )

    range_check_df = remove_outliers_df1.filter(
        (F.col("year") > 0) &
        (F.col("condition") > 0) &
        (F.col("odometer") > 0) &
        (F.col("mmr") > 1) &
        (F.col("sellingprice") > 1)
    )

    removed_hyphens_from_df = range_check_df.filter(
        ~(F.col("color").rlike("[-–—]") | F.col("interior").rlike("[-–—]"))
    )

    normalized_date_df = removed_hyphens_from_df.withColumn(
        "saledate",
        F.to_timestamp(
            F.trim(  # Trim any leading or trailing whitespace
                F.regexp_replace(
                    F.regexp_replace(F.col("saledate"), "GMT[-+]?\\d{4}\\s*\\(.*\\)", ""),  # Remove GMT and timezone info
                    "^\\w{3}\\s+", ""  # Remove the day of the week
                )
            ),
            "MMM dd yyyy HH:mm:ss"  # Adjusted format to parse
        )
    )
    # Continue with other processing...
    formatted_date_df = normalized_date_df.withColumn(
        "saledate",
        F.date_format(F.col("saledate"), "yyyy-MM-dd HH:mm:ss")  # Format for output if needed
    )

    drop_timestamp_df = formatted_date_df.drop("created_at")

    final_df = drop_timestamp_df.orderBy("state")
    
    final_df.printSchema()
    return final_df

def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    jdbc_url = "jdbc:postgresql://ec2-18-132-73-146.eu-west-2.compute.amazonaws.com:5432/testdb"
    connection_properties = {
        "user": "consultants",
        "password": "WelcomeItc@2022",
        "driver": "org.postgresql.Driver"
    }

    df_vehicle_sales = spark.read.jdbc(jdbc_url, "vehicle_sales", properties=connection_properties)

    max_timestamp = df_vehicle_sales.agg(F.max("created_at")).first()[0]

    df_timestamp = spark.read.jdbc(jdbc_url, "timestamp_table", properties=connection_properties)

    max_timestamp_from_time_table = df_timestamp.agg(F.max("created_at")).first()[0]

    incremental_load_df = df_vehicle_sales.filter(F.col("created_at") > max_timestamp_from_time_table)

    new_max_timestamp = df_vehicle_sales.agg(F.max("created_at")).first()[0]
    
    s3_path = "s3://myvehiclesalesproject/stagingsilver/toprocess"

    if new_max_timestamp is not None and max_timestamp_from_time_table is not None:
        if new_max_timestamp == max_timestamp_from_time_table:
            print("No change")
        elif new_max_timestamp > max_timestamp_from_time_table:
            cleaned_df = cleaning_data(incremental_load_df)
            cleaned_df = cleaned_df.withColumn("year", cleaned_df["year"].cast(IntegerType()))
            cleaned_df = cleaned_df.withColumn("sellingprice", cleaned_df["sellingprice"].cast(DoubleType()))
            cleaned_df = cleaned_df.withColumn("mmr", cleaned_df["mmr"].cast(DoubleType()))
            cleaned_df = cleaned_df.withColumn("condition", cleaned_df["condition"].cast(DoubleType()))
            cleaned_df = cleaned_df.withColumn("odometer", cleaned_df["odometer"].cast(IntegerType()))
            cleaned_df = cleaned_df.withColumn("saledate", cleaned_df["saledate"].cast(TimestampType()))
            #cleaned_df.coalesce(1).write.mode("append").csv(s3_path)
            cleaned_dyf = DynamicFrame.fromDF(cleaned_df, glueContext, "cleaned_dyf")

            # Write back as DynamicFrame
            glueContext.write_dynamic_frame.from_options(cleaned_dyf, connection_type="s3", connection_options={"path": s3_path}, format="parquet")
            # Create a DataFrame for the new timestamp
            new_timestamp_df = spark.createDataFrame([(new_max_timestamp,)], ["created_at"])

            # Write the new timestamp DataFrame back to the database
            new_timestamp_df.write.jdbc(jdbc_url, "timestamp_table", mode="append", properties=connection_properties)

        else:
            print(f"MaxTimestampFromTimeTable ({max_timestamp_from_time_table}) is greater.")
    else:
        print("One or both timestamps are null.")

    job.commit()

if __name__ == "__main__":
    main()
