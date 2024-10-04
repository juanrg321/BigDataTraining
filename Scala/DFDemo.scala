package itc

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, lit, lower, trim}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object DFDemo extends App {

  val sparkconf = new SparkConf()

  sparkconf.set("spark.app.name", "DataFrameDemo")

  sparkconf.set("spark.master", "local[1]")

  val ss = SparkSession.builder().config(sparkconf).getOrCreate()

  val productSchema = StructType(Array(
    StructField("product_number", StringType, nullable = true),
    StructField("product_name", StringType, nullable = true),
    StructField("product_category", StringType, nullable = true),
    StructField("product_scale", StringType, nullable = true),
    StructField("product_manufacturer", StringType, nullable = true),
    StructField("product_description", StringType, nullable = true),
    StructField("length", DoubleType, nullable = true),
    StructField("width", DoubleType, nullable = true),
    StructField("height", DoubleType, nullable = true)
  ))

  val ddlSchema =
    """
      product_number STRING,
      product_name STRING,
      product_category STRING,
      product_scale STRING,
      product_manufacturer STRING,
      product_description STRING,
      length DOUBLE,
      width DOUBLE,
      height DOUBLE
  """

  //val productdf = ss.read.option("header", true).option("inferSchema", true).csv("C:/demos/products.csv")

  val productdf = ss.read.option("header", true).schema(ddlSchema).csv("C:/Amazon/Raw/products.csv")

  val cleanedDF = productdf.dropDuplicates("product_number")
    .na.fill("Uknown", Seq("product_name"))
    .na.fill("0", Seq("length", "width", "height"))
    .withColumn("product_name", trim(lower(col("product_name"))))
    .drop("product_scale", "product_description")
    .filter(col("length") > 0 && col("width") > 0)
    .withColumn("length", col("length").cast("Float"))
    .withColumn("col1", lit(100))
    .withColumn("col2", col("length") * col("width"))

  cleanedDF.show(5)
  cleanedDF.coalesce(1).write.csv("C:/Amazon/Staging/cleaned2")

  //productdf.show(5)
}
