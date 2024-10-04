package itc

import org.apache.spark.SparkContext

object greatThan50 extends App{

  val sc = new SparkContext("local[1]", "GreaterThan50")

  //Read File

  val rdd1 = sc.textFile("C:/Users/Juan Granillo/Documents/greaterThan50.txt")

  // Read line by line and split with space
  val sensorCnt = rdd1.map(line => {
    val parts = line.split(",")
    val sensorId = parts(0)
    val temp = parts(2).toDouble
    (sensorId, if (temp > 50) 1 else 0)
  })
    .reduceByKey(_+_).sortBy(_._2, false)

  sensorCnt.collect().foreach{  case (sensorId, count) =>
    println(s"$count, $sensorId")}

}
