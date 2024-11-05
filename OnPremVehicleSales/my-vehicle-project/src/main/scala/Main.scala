import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, lower, mean, regexp_replace, stddev, to_date, to_timestamp, trim}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import java.sql.DriverManager

object Main {

  def cleaningData (dfVehicleSales: DataFrame): DataFrame = {
    val nullValuesDroppedDf = dfVehicleSales.na.drop()

    //nullValuesDroppedDf.show();

    val trimmedDf = nullValuesDroppedDf.select(nullValuesDroppedDf.columns.filter(_ != "saledate").map(c => trim(col(c)).alias(c)) :+ col("saledate"): _*)
    //trimmedDf.show()

    val toLowerDF = trimmedDf.select(
      trimmedDf.columns.filter(_ != "saledate").map(c => lower(col(c)).alias(c)) :+ col("saledate"): _*)

    //toLowerDF.show()

    val removeDuplicatesDf: DataFrame = toLowerDF.dropDuplicates(Seq("vin"))

    //removeDuplicatesDf.show()
    //removeDuplicatesDf.createOrReplaceTempView("vehicle_sales")
    //ss.sql("select make, model, sellingprice from vehicle_sales where sellingprice > 150000 order by sellingprice DESC").show(5)

    val removeOutliers1Df: DataFrame = removeDuplicatesDf.filter(
      !(
        col("make") === "ford" &&
          col("model") === "escape" &&
          col("sellingprice") > 150000
        )
    )
    //removeOutliersDf1.createOrReplaceTempView("vehicle_sales")
    //ss.sql("select make, model, sellingprice from vehicle_sales where sellingprice > 150000 order by sellingprice DESC").show(5)
    val rangeCheckDf: DataFrame = removeOutliers1Df.filter(
      (
          col("year") > 0 &&
          col("condition") > 0 &&
          col("odometer") > 0 &&
          col("mmr") > 1 &&
          col("sellingprice") > 1
        )
    )
    import org.apache.spark.sql.functions._

    val removedHyphensFromDf = rangeCheckDf.filter(
      !(col("color").rlike("[-–—]") || col("interior").rlike("[-–—]"))
    )
    val normalizedDateDf = removedHyphensFromDf.withColumn("saledate",
      to_timestamp(
        regexp_replace(col("saledate"), "\\sGMT[-+]\\d{4}\\s\\(.*\\)", ""), // Remove GMT and timezone info
        "EEE MMM dd yyyy HH:mm:ss" // Specify the date format
      )
    )
    val formattedDateDf = normalizedDateDf.withColumn("saledate",
      date_format(col("saledate"), "yyyy-MM-dd HH:mm:ss")
    )
    val dropTimeStampDf = formattedDateDf.drop("created_at")

//    val sellerWithStateDf = dropTimeStampDf.withColumn("seller",
//      concat_ws(":", col("seller"), col("state")) // Concatenate seller and state, separated by a colon
//    )
    //val orderByVinDf = dropTimeStampDf.orderBy("vin")

    //val limit10000Df = orderByVinDf.limit(5000)

    val finalDf = dropTimeStampDf.orderBy("state")

    finalDf
  }
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .appName("psql") // Use all available cores
      .getOrCreate()

    ss.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    val jdbcUrl = "jdbc:postgresql://ec2-18-132-73-146.eu-west-2.compute.amazonaws.com:5432/testdb"

    val connectProperties = new java.util.Properties()

    val password = "WelcomeItc@2022"
    connectProperties.put("user", "consultants")
    connectProperties.put("password", password)
    connectProperties.put("driver", "org.postgresql.Driver")


    val dfVehicleSales = ss.read.jdbc(jdbcUrl, "vehicle_sales", connectProperties)

    //dfVehicleSales.show()
    val maxTimestamp = dfVehicleSales.agg(functions.max("created_at")).first().getTimestamp(0)

    // Create the connection to the database
    val dfTimestamp = ss.read.jdbc(jdbcUrl, "timestamp_table", connectProperties)

    // Get the maximum timestamp from table
    val MaxTimestampFromTimeTable = dfTimestamp.agg(functions.max("created_at")).first().getTimestamp(0)

    val incrementalLoadDf: DataFrame = dfVehicleSales.filter(col("created_at") > MaxTimestampFromTimeTable)

    val newMaxTimestamp = dfVehicleSales.agg(functions.max("created_at")).first().getTimestamp(0)
    val hdfsPath = "/tmp/vehicle_sales_curated"
    var mutex = false;
    if (newMaxTimestamp != null && MaxTimestampFromTimeTable != null) {
      val comparisonResult = newMaxTimestamp.compareTo(MaxTimestampFromTimeTable)

      comparisonResult match {
        case 0 =>
            println("No change")
        case _ if comparisonResult > 0 =>
          val cleanedDF = cleaningData(incrementalLoadDf)
          cleanedDF.coalesce(1).write.mode("append").csv(hdfsPath)
          mutex = true
          while(mutex){
            val connection = DriverManager.getConnection(jdbcUrl, connectProperties)

            // Prepare the SQL insert statement
            val insertSQL = "INSERT INTO timestamp_table (created_at) VALUES (?)"

            // Create a PreparedStatement
            val preparedStatement = connection.prepareStatement(insertSQL)
            preparedStatement.setTimestamp(1, newMaxTimestamp)

            // Execute the insert
            preparedStatement.executeUpdate()

            // Clean up resources
            preparedStatement.close()
            connection.close()
            mutex = false
          }
        case _ =>
          println(s"MaxTimestampFromTimeTable ($MaxTimestampFromTimeTable) is greater.")
      }
    } else {
      println("One or both timestamps are null.")
    }
  }
}