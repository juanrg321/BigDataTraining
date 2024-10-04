package itc
import org.apache.spark.SparkContext


object Main {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[1]", "appName")

    //Read File

    val rdd1 = sc.textFile("C:/Users/Juan Granillo/Desktop/Assignment/myfile2.txt")

    // Read line by line and split with space
    val words = rdd1.flatMap(line => line.split(" ")) // transformation

    // count words
    val word = words.map(word => (word,1))

    //aggregation
    val wordcount = word.reduceByKey((x,y) => x+y).sortBy(_._2, false)
    // RuduceByKey returns RDD
    val wordCnt1 = word.countByKey() //action
    // countByKey() returns a map of key and value
   // wordcount.collect().foreach(println) // action
    println(wordCnt1)
    //wordcount.saveAsTextFile("C://demos/result2") //action
  }
}