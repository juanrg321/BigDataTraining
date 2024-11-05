from kafka import KafkaProducer
from kafka.errors import KafkaError

# Define the Kafka broker addresses
bootstrap_servers = [
    "ip-172-31-13-101.eu-west-2.compute.internal:9092",
    "ip-172-31-1-36.eu-west-2.compute.internal:9092",
    "ip-172-31-5-217.eu-west-2.compute.internal:9092",
    "ip-172-31-9-237.eu-west-2.compute.internal:9092"
]

# Average temperatures data
average_temperatures = {
  "AL": "64.0",   # Alabama
  "AK": "26.6",   # Alaska
  "AZ": "72.3",   # Arizona
  "AR": "61.7",   # Arkansas
  "CA": "59.4",   # California
  "CO": "45.6",   # Colorado
  "CT": "51.9",   # Connecticut
  "DE": "55.5",   # Delaware
  "FL": "70.7",   # Florida
  "GA": "64.5",   # Georgia
  "HI": "70.0",   # Hawaii
  "ID": "49.6",   # Idaho
  "IL": "52.8",   # Illinois
  "IN": "54.4",   # Indiana
  "IA": "50.1",   # Iowa
  "KS": "56.1",   # Kansas
  "KY": "55.2",   # Kentucky
  "LA": "67.1",   # Louisiana
  "ME": "45.2",   # Maine
  "MD": "54.9",   # Maryland
  "MA": "51.0",   # Massachusetts
  "MI": "48.6",   # Michigan
  "MN": "43.6",   # Minnesota
  "MS": "65.3",   # Mississippi
  "MO": "56.9",   # Missouri
  "MT": "42.4",   # Montana
  "NE": "50.5",   # Nebraska
  "NV": "56.5",   # Nevada
  "NH": "47.9",   # New Hampshire
  "NJ": "54.5",   # New Jersey
  "NM": "61.6",   # New Mexico
  "NY": "49.8",   # New York
  "NC": "61.2",   # North Carolina
  "ND": "41.3",   # North Dakota
  "OH": "52.5",   # Ohio
  "OK": "60.8",   # Oklahoma
  "OR": "52.8",   # Oregon
  "PA": "50.3",   # Pennsylvania
  "RI": "54.6",   # Rhode Island
  "SC": "63.5",   # South Carolina
  "SD": "47.0",   # South Dakota
  "TN": "59.8",   # Tennessee
  "TX": "64.8",   # Texas
  "UT": "51.4",   # Utah
  "VT": "45.0",   # Vermont
  "VA": "56.5",   # Virginia
  "WA": "52.0",   # Washington
  "WV": "53.5",   # West Virginia
  "WI": "45.2",   # Wisconsin
  "WY": "42.0"    # Wyoming
}


# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         key_serializer=str.encode,
                         value_serializer=str.encode)

# Define the Kafka topic
topic = "avg_state_temp"

# Broadcast average temperatures
for state, temp in average_temperatures.items():
    try:
        future = producer.send(topic, key=state, value=temp)
        metadata = future.get(timeout=10)  # Wait for confirmation
        print(f"Successfully sent record: {state} -> {temp} to partition {metadata.partition} at offset {metadata.offset}")
    except KafkaError as e:
        print(f"Error sending record for {state}: {e}")

# Close the producer
producer.close()
print("Finished sending temperature data.")

