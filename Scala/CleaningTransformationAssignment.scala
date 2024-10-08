package itc

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, lit, lower, mean, stddev, trim}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object CleaningTransformationAssignment extends App {
  val sparkconf = new SparkConf()

  sparkconf.set("spark.app.name", "DataFrameDemo")

  sparkconf.set("spark.master", "local[1]")

  val ss = SparkSession.builder().config(sparkconf).getOrCreate()
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

  val productdf = ss.read.option("header", true).schema(ddlSchema).csv("C:/Amazon/Raw/products.csv")

  val lengthColumn = "length"
  val widthColumn = "width"

  // Create stats df with with mean and stddev of length and width
  val stats = productdf.select(
    mean(lengthColumn).alias("mean_length"),
    stddev(lengthColumn).alias("stddev_length"),
    mean(widthColumn).alias("mean_width"),
    stddev(widthColumn).alias("stddev_length")
  ).first()
  // Save results into variables
  val meanLength = stats.getDouble(0)
  val stddevLength= stats.getDouble(1)
  val meanWidth = stats.getDouble(2)
  val stddevWidth = stats.getDouble(3)
  // Find upper and lower bound for mean and length
  val lengthLowerBound = meanLength - 3 * stddevLength
  val lengthUpperBound = meanLength + 3 * stddevLength
  val widthLowerBound = meanWidth - 3 * stddevWidth
  val widthUpperBound = meanWidth + 3 * stddevWidth
  // Filter results between upperbound and lower bound into new  filtered DF
  val filteredDF = productdf.filter(col(lengthColumn).between(lengthLowerBound, lengthUpperBound)
  &&
  col(widthColumn).between(widthLowerBound, widthUpperBound)
  )
  // Display results
  filteredDF.show()
  // Filter results that were not between upper and lower bound into new outliers DF
  val outliersDF = productdf.filter(!(col(lengthColumn).between(lengthLowerBound, lengthUpperBound)
    &&
    col(widthColumn).between(widthLowerBound, widthUpperBound))
  )
  // Display results of outliers
  //outliersDF.show()

  val splitDF = filteredDF.withColumn("storeid", split(col("product_number"),"_").getItem(0))
    .withColumn("productid", split(col("product_number"), "_").getItem(1))

  splitDF.show()

  val splitNameintoYearDF = splitDF.withColumn("year", split(col("product_name")," ").getItem(0))

  splitNameintoYearDF.show()

  val dfwithProductSize = splitNameintoYearDF.withColumn("product_size",
    when(col("length") < 1000, "Small")
      .when(col("length").between(1000, 2000), "Medium")
      .when(col("length").between(2001, 3000), "Large")
      .otherwise("Large")
  )

  dfwithProductSize.show()

  val res1 = dfwithProductSize.groupBy("product_category")
    .pivot("product_size")
    .agg(count("product_number"))
    .na.fill(0)

  res1.show()

  val windowSpec = Window.partitionBy("product_category")
    .orderBy(col("length").desc)

  val rankedDF = dfwithProductSize.withColumn("rank", rank().over(windowSpec))

  val secondLongestDF = rankedDF.filter(col("rank") === 2)

  secondLongestDF.select("product_category", "product_name", "length").show()
}
