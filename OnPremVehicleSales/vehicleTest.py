from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType, DoubleType

# Initialize Spark session
spark = SparkSession.builder.appName("DataQualityCheck").getOrCreate()

# Load the CSV file from HDFS
df = spark.read.csv("/tmp/vehicle_sales_curated", header=True, inferSchema=True)

# Function to handle column names with special characters
def safe_col_name(column_name):
    """Ensure column names with special characters or spaces are enclosed in backticks."""
    return f"`{column_name}`" if not column_name.isidentifier() else column_name

# Data quality check function
def check_data_quality():
    # 1. Check for null values in each column
    null_count_list = {}
    for column in df.columns:
        null_count = 0  # Initialize counter for null values
        for row in df.select(safe_col_name(column)).collect():
            if row[0] is None:
                null_count += 1
        null_count_list[column] = null_count

    # 2. Check for duplicate records
    total_count = df.count()
    distinct_count = df.dropDuplicates().count()
    duplicate_count = total_count - distinct_count

    # 3. Check for negative values in numerical columns
    negative_value_columns = {}
    numerical_columns = []  # List to hold numerical columns

    # Check the first row to determine the data type for each column
    first_row = df.first()
    for index, column in enumerate(df.columns):
        value = first_row[index]
        if isinstance(value, (int, float)):  # Check for numerical types
            numerical_columns.append(column)

    # Count negative values in numerical columns
    for column in numerical_columns:
        negative_count = 0  # Initialize counter for negative values
        for row in df.select(safe_col_name(column)).collect():
            if row[0] is not None and row[0] < 0:  # Ensure value is not None
                negative_count += 1
        negative_value_columns[column] = negative_count

    # Prepare output
    output = []

    output.append("1. Null Values Check:\n")
    for column, count in null_count_list.items():
        output.append(f"Column: {column}, Null Count: {count}\n")

    output.append("\n2. Duplicate Records Check:\n")
    output.append(f"Duplicate Record Count: {duplicate_count}\n")

    output.append("\n3. Negative Values Check (in numerical columns):\n")
    for column, count in negative_value_columns.items():
        output.append(f"Column: {column}, Negative Value Count: {count}\n")

    # Write output to a file on HDFS
    output_rdd = spark.sparkContext.parallelize(output)
    output_rdd.saveAsTextFile("/tmp/vehicleResult")

# Run the data quality check function
check_data_quality()

# Stop the Spark session
spark.stop()

