package com.ubs.spark.sparkjobs.assignment

import org.apache.spark._
import org.apache.spark.sql.{ SQLContext, SparkSession }
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import com.ubs.spark.CommonUtil
import org.apache.spark.sql.catalyst.ScalaReflection

object UbsCodingAssignment {

  var spark: SparkSession = null
  def main(args: Array[String]) {

    try {
      spark = CommonUtil.buildSparkSession()

      //Read Input Start Of Day Positions file
      val csvFilePath = "src/main/resources/Input_StartOfDay_Positions.txt"
      var startOfDayPositionDF = CommonUtil.readFromCSV(csvFilePath, spark, ScalaReflection.schemaFor[CommonUtil.start_day_position].dataType.asInstanceOf[StructType])
      
      //Read the JSON transaction file
      val jsonFilePath = "src/main/resources/1537277231233_Input_Transactions.txt"
      var transactionDF = CommonUtil.readFromJson(jsonFilePath, spark, ScalaReflection.schemaFor[CommonUtil.input_transaction].dataType.asInstanceOf[StructType])

      //create two different columns for easy calculation and understanding
      transactionDF = transactionDF
                        .withColumn("B", when(col("TransactionType").equalTo("B"), col("TransactionQuantity")).otherwise(0))
                        .withColumn("S", when(col("TransactionType").equalTo("S"), col("TransactionQuantity")).otherwise(0))

      //calculating the sum by grouping Instrument                  
      transactionDF = transactionDF.groupBy(col("Instrument")).agg(sum(col("B")) as "totalB", sum(col("S")) as "totalS")

      val evaluatedDF = evaluateTransaction(startOfDayPositionDF, transactionDF)
      
      //write output
      val outputFile = "src/main/resources/Expected_EndOfDay_Positions.csv"
      val spark2 = spark
      import spark2.implicits._
      CommonUtil.saveToCSV[CommonUtil.end_day_position](outputFile, evaluatedDF.as[CommonUtil.end_day_position])
      
    } catch {
      case t: Throwable => t.printStackTrace
    } finally {
      if (null != spark) {
        spark.stop()
      }
    }
  }
  
  def evaluateTransaction(startOfDayPositionDF: DataFrame, transactionDF: DataFrame): DataFrame ={
    //join startOfDayPositionDF and txn DFs to calculate updated quantity and Delta
    val spark = CommonUtil.buildSparkSession()
    import spark.implicits._
    val outputDF = startOfDayPositionDF
      .join(transactionDF, Seq("Instrument"), "LeftOuter")
      .withColumn("Quantity", when(col("AccountType").equalTo("E") && col("totalB").isNotNull && col("totalS").isNotNull, col("Quantity") + col("totalB") - col("totalS"))
        .otherwise(when(col("AccountType").equalTo("I") && col("totalB").isNotNull && col("totalS").isNotNull, col("Quantity") - col("totalB") + col("totalS")).otherwise(col("Quantity"))))
      .withColumn("Delta", when(col("AccountType").equalTo("E") && col("totalB").isNotNull && col("totalS").isNotNull, col("totalB") - col("totalS"))
        .otherwise(when(col("AccountType").equalTo("I") && col("totalB").isNotNull && col("totalS").isNotNull, col("totalS") - col("totalB")).otherwise(0)))
      .drop(col("totalB")).drop(col("totalS"))
      .coalesce(1)
      .withColumn("Delta1", when(col("Delta").lt(0), col("Delta") * -1).otherwise(col("Delta")))
      .orderBy(col("Delta1").desc, col("Instrument").desc).drop(col("Delta1"))
    //    outputDF.show()
    outputDF
  }
}