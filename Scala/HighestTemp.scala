package itc

import org.apache.spark.SparkContext

object HighestTemp extends App{

  val sc = new SparkContext("local[1]", "HighestTemp")

  //Read File

  val rdd1 = sc.textFile("C:/Users/Juan Granillo/Documents/HighestTemp.txt")

  // Read line by line and split with space
  val highestTemp = rdd1.map(line => line.split(",")(2).toDouble).max()

  println(s"Highest temp: $highestTemp")

  //wordcount.saveAsTextFile("C://demos/result2")
}
