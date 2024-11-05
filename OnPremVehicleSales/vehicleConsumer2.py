from pyspark.sql import SparkSession
from pyspark.sql import Row
from kafka import KafkaConsumer, TopicPartition

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaToHDFS") \
    .getOrCreate()

# Kafka consumer setup
consumer = KafkaConsumer(
     bootstrap_servers=[
        'ip-172-31-13-101.eu-west-2.compute.internal:9092',
        'ip-172-31-1-36.eu-west-2.compute.internal:9092',
        'ip-172-31-9-237.eu-west-2.compute.internal:9092'
    ],
    group_id=None,
    key_deserializer=lambda k: k.decode('utf-8'),
    value_deserializer=lambda v: v.decode('utf-8'),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)
topic = 'tempdemo123'
partition = 0
consumer.assign([TopicPartition(topic,partition)])
# List to hold rows for Spark DataFrame
rows = []
try:
    for message in consumer:
        state = message.key.lower()
        temperature = message.value
        
        # Create a Row object for each message
        row = Row(state=state, temperature=temperature)
        df = spark.createDataFrame([row])  # Create a DataFrame from the single Row object
        
        # Write to HDFS directly
        df.write.mode("append").csv("hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8020/tmp/vehicle_temp_fin")
        print(f"Wrote record to HDFS: {state}, {temperature}")

except Exception as e:
    print(f"Error: {e}")

finally:
    consumer.close()
    spark.stop()

