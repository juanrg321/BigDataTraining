import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, Callback, RecordMetadata}
  val props = new Properties()
  props.put("bootstrap.servers", "ip-172-31-13-101.eu-west-2.compute.internal:9092, ip-172-31-1-36.eu-west-2.compute.internal:9092, ip-172-31-5-217.eu-west-2.compute.internal:9092, ip-172-31-9-237.eu-west-2.compute.internal:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

val averageTemperatures: Map[String, String] = Map(
  "Alabama" -> "64.0",
  "Alaska" -> "26.6",
  "Arizona" -> "72.3",
  "Arkansas" -> "61.7",
  "California" -> "59.4",
  "Colorado" -> "45.6",
  "Connecticut" -> "51.9",
  "Delaware" -> "55.5",
  "Florida" -> "70.7",
  "Georgia" -> "64.5",
  "Hawaii" -> "70.0",
  "Idaho" -> "49.6",
  "Illinois" -> "52.8",
  "Indiana" -> "54.4",
  "Iowa" -> "50.1",
  "Kansas" -> "56.1",
  "Kentucky" -> "55.2",
  "Louisiana" -> "67.1",
  "Maine" -> "45.2",
  "Maryland" -> "54.9",
  "Massachusetts" -> "51.0",
  "Michigan" -> "48.6",
  "Minnesota" -> "43.6",
  "Mississippi" -> "65.3",
  "Missouri" -> "56.9",
  "Montana" -> "42.4",
  "Nebraska" -> "50.5",
  "Nevada" -> "56.5",
  "New Hampshire" -> "47.9",
  "New Jersey" -> "54.5",
  "New Mexico" -> "61.6",
  "New York" -> "49.8",
  "North Carolina" -> "61.2",
  "North Dakota" -> "41.3",
  "Ohio" -> "52.5",
  "Oklahoma" -> "60.8",
  "Oregon" -> "52.8",
  "Pennsylvania" -> "50.3",
  "Rhode Island" -> "54.6",
  "South Carolina" -> "63.5",
  "South Dakota" -> "47.0",
  "Tennessee" -> "59.8",
  "Texas" -> "64.8",
  "Utah" -> "51.4",
  "Vermont" -> "45.0",
  "Virginia" ->" 56.5",
  "Washington" -> "52.0",
  "West Virginia" -> "53.5",
  "Wisconsin" -> "45.2",
  "Wyoming" -> "42.0"
)
  // Create Kafka producer
  val producer = new KafkaProducer[String, String](props)
  val topic = "test123"

    // Broadcast average temperatures
  averageTemperatures.foreach { case (state, temp) =>
    val record = new ProducerRecord[String, String](topic, state, temp.toString)
    producer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception == null) {
          println(s"Successfully sent record: ${state} -> ${temp} to partition ${metadata.partition()} at offset ${metadata.offset()}")
        } else {
          println(s"Error sending record for ${state}: ${exception.getMessage}")
        }
      }
    })
  }

  // Close the producer
  producer.close()
  println("Finished sending temperature data.")



