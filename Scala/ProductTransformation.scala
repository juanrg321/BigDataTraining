package itc

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, first, sum}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object ProductTransformation extends App {
  val sparkconf = new SparkConf()

  sparkconf.set("spark.app.name", "DataFrameDemo")

  sparkconf.set("spark.master", "local[1]")

  val ss = SparkSession.builder().config(sparkconf).getOrCreate()


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

  //val productdf = ss.read.option("header", true).option("inferSchema", true).csv("C:/demos/products.csv")

  val productdf = ss.read.option("header", true).schema(ddlSchema).csv("C:/Amazon/Staging/cleaned2/part-00000-548d2b8f-a7a2-4929-bb2a-283d5b82d595-c000.csv")

  val resultdf = productdf.groupBy("product_category").agg(avg("width").as("Avg_Width"), sum("length").as("Sum_Length"))
    .orderBy("product_category")
  //resultdf.show()

  val res1 = productdf.groupBy("product_number")
    .pivot("product_category")
    .agg(first("product_name"))

  val res2 = productdf.groupBy("product_category")
    .pivot("product_number")
    .agg(avg("length"))

  res2.show()
}
