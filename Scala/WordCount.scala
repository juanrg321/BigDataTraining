package itc

import org.apache.spark.SparkContext

object WordCount extends App {

  val sc = new SparkContext("local", "appName")

  val rdd1 =sc.textFile("C:/Users/Juan Granillo/Desktop/Assignment/myfile2.txt")

  val words = rdd1.flatMap(line => line.split(" "))

  val word = words.map(word => (word,1))

  val wordCnt = word.reduceByKey((x,y) => x+y)

  wordCnt.collect().foreach(println)


}

