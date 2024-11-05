import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, col, dayofmonth, dense_rank, lit, month, quarter, to_timestamp, udf, year}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import scala.util.Random
import java.util.UUID
object Main {
  def generateShortId1(length: Int): String = {
    val characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    (1 to length).map(_ => characters(Random.nextInt(characters.length))).mkString
  }
  def generateShortId2(length: Int): String = {
    val characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    (1 to length).map(_ => characters(Random.nextInt(characters.length))).mkString
  }
  def generateShortId3(length: Int): String = {
    val characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    (1 to length).map(_ => characters(Random.nextInt(characters.length))).mkString
  }
  def main(args: Array[String]): Unit = {
    val schema = StructType(Array(
      StructField("year", IntegerType, true),
      StructField("make", StringType, true),
      StructField("model", StringType, true),
      StructField("trim", StringType, true),
      StructField("body", StringType, true),
      StructField("transmission", StringType, true),
      StructField("vin", StringType, true),
      StructField("state", StringType, true),
      StructField("condition", DoubleType, true),
      StructField("odometer", DoubleType, true),
      StructField("color", StringType, true),
      StructField("interior", StringType, true),
      StructField("seller", StringType, true),
      StructField("mmr", DoubleType, true),
      StructField("sellingprice", DoubleType, true),
      StructField("saledate", StringType, true),
      // Use StringType initially if date format is irregular
    ))
    val dimDateSchema = StructType(Array(
      StructField("year", IntegerType, true), // Year of the sale
      StructField("month", StringType, true), // Month of the sale (as a string)
      StructField("day", StringType, true), // Day of the sale (as a string)
      StructField("quarter", StringType, true), // Quarter of the sale (as a string)
      StructField("saledate", StringType, true), // Original saledate
      StructField("date_id", StringType, true) // Unique primary key
    ))
    val vehicleSchema = StructType(Array(
      StructField("vin", StringType, true),
      StructField("make", StringType, true),
      StructField("model", StringType, true),
      StructField("trim", StringType, true),
      StructField("body", StringType, true),
      StructField("transmission", StringType, true),
      StructField("color", StringType, true),
      StructField("interior", StringType, true),
      StructField("year", IntegerType, true),
    ))
    val sellerSchema = StructType(Array(
      StructField("seller", StringType, true),
      StructField("state", StringType, true),
      StructField("seller_id", StringType, true)
    ))
    val ss = SparkSession.builder().appName("Dimensional Modelling").getOrCreate()
    val generateShortIdUDF1 = udf(() => generateShortId1(4))
    val generateShortIdUDF2 = udf(() => generateShortId2(4))
    val generateShortIdUDF3 = udf(() => generateShortId3(4))
    val hdfsPathInput = "/tmp/vehicle_sales_curated"
    val hdfsPathOutputSellers = "/tmp/dim_sellers"
    val hdfsPathOutputDate = "/tmp/dim_date_vehicles"
    val hdfsPathOutputVehicles = "/tmp/dim_vehicles"
    val hdfsPathOutputFactTable = "/tmp/dim_vehicle_fact"

    val df = ss.read
      .option("header", "true") // Set to true if your file has a header
      .schema(schema) // Automatically infer the data types
      .csv(hdfsPathInput)

    // **New Step: Generate distinct date_id and seller_id**
    // Generate distinct date_id for each unique "saledate"
    val dateWindow = Window.partitionBy("saledate").orderBy("saledate")
    val sellerWindow = Window.partitionBy("seller").orderBy("seller")

    // Generate the IDs in a single step and cache the result
    val dfWithIds = df
      .withColumn("date_id", dense_rank().over(dateWindow))
      .withColumn("date_id", generateShortIdUDF1())
      .withColumn("seller_id", dense_rank().over(sellerWindow))
      .withColumn("seller_id", generateShortIdUDF2())
      .cache() // Cache this DataFrame to reuse it across both fact and dimension tables

    val parsedDf = dfWithIds.withColumn("saledate", to_timestamp(col("saledate")))
    // Generate the dimension tables using the cached DataFrame
    val dimDateDf = parsedDf
      .withColumn("year", year(col("saledate")))
      .withColumn("month", month(col("saledate")))
      .withColumn("day", dayofmonth(col("saledate")))
      .withColumn("quarter", quarter(col("saledate")))
      .select("year", "month", "day", "quarter", "saledate", "date_id")

    val dimSellerdf = parsedDf.select("seller", "state", "seller_id")
    val dimVehiclesDf = parsedDf.select("vin", "make", "model", "trim", "body", "transmission", "color", "interior", "year")

    dimDateDf.coalesce(1).write.mode("overwrite").csv(hdfsPathOutputDate)
    dimSellerdf.coalesce(1).write.mode("overwrite").csv(hdfsPathOutputSellers)
    dimVehiclesDf.coalesce(1).write.mode("overwrite").csv(hdfsPathOutputVehicles)

    // 2009,black,65033.0,2014-12-31 10:00:00,15700.0,32.0,ed napleton honda,14500.0,""

    val dimVehicleDf = ss.read.option("header", "true").schema(vehicleSchema).csv(hdfsPathOutputVehicles)
    val dimSellersDf = ss.read.option("header", "true").schema(sellerSchema).csv(hdfsPathOutputSellers)
    val dimTimeDf = ss.read.option("header", "true").schema(dimDateSchema).csv(hdfsPathOutputDate)

    val timestampDf = df.withColumn("saledate", to_timestamp(col("saledate"), "yyyy-MM-dd HH:mm:ss"))

    //RqZF56,19uua8f50ca017655,mQrIus,ciKLn4,20500.0,2014-12-30T15:00:00.000Z,42227.0,42.0
    val populatedFactDf = parsedDf.select("vin","sellingprice", "saledate","odometer","condition","date_id", "seller_id")

    val saleWindow = Window.partitionBy("vin").orderBy("vin")
    val populatedFactwithIDDf = populatedFactDf.withColumn("sale_id", dense_rank().over(saleWindow))
      .withColumn("sale_id", generateShortIdUDF3())

    populatedFactwithIDDf.coalesce(1).write.mode("overwrite").csv(hdfsPathOutputFactTable)
  }
}