package itc

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, first, rank, sum, when}

object TransformationAssignment extends App {

  val sparkConf = new SparkConf()

  sparkConf.set("spark.app.name", "DataFrameDemo")

  sparkConf.set("spark.master", "local[1]")

  val ss = SparkSession.builder().config(sparkConf).getOrCreate()

  val ddlSchema =
    """
      product_number STRING,
      product_name STRING,
      product_category STRING,
      product_manufacturer STRING,
      length DOUBLE,
      width DOUBLE,
      height DOUBLE,
      col1 INT,
      col2 DOUBLE
  """

  val productdf = ss.read.option("header", true).schema(ddlSchema).csv("C:/Amazon/Staging/cleaned2/part-00000-548d2b8f-a7a2-4929-bb2a-283d5b82d595-c000.csv")

  val dfwithProductSize = productdf.withColumn("product_size",
  when(col("length") < 1000, "Small")
    .when(col("length").between(1000, 2000), "Medium")
    .when(col("length").between(2001, 3000), "Large")
    .otherwise("Large")
  )

  //dfwithProductSize.show()

  val res1 = dfwithProductSize.groupBy("product_category")
    .pivot("product_size")
    .agg(count("product_number"))
    .na.fill(0)

  //res1.show()

  val windowSpec = Window.partitionBy("product_category")
    .orderBy(col("length").desc)

  val rankedDF = productdf.withColumn("rank", rank().over(windowSpec))

  val secondLongestDF = rankedDF.filter(col("rank") === 2)

  secondLongestDF.select("product_category", "product_name", "length").show()
}
