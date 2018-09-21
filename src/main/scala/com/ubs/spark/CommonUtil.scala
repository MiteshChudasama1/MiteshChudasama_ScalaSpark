package com.ubs.spark

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object CommonUtil {

  def buildSparkSession(): SparkSession = {
    val str = "local[2]"
    val sparkConf = new SparkConf().setMaster(str).setAppName("UbsCodingAssignment")

    var ssb = SparkSession.builder.config(sparkConf)
    val spark: SparkSession = ssb.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  def readFromCSV(filePath: String, spark: SparkSession, schema: StructType): DataFrame = {
    if (filePath == null || filePath.trim() == "") {
      throw new RuntimeException("filePath is null or blank")
    } else if (spark == null) {
      throw new RuntimeException("spark session is null")
    } else {
      var csvDF = spark.read
        .format("csv")
        .schema(schema)
        .option("header", true)
        .option("delimiter", ",")
        .option("inferSchema", "true")
        .load(filePath)
      csvDF
    }
  }

  def readFromJson(filePath: String, spark: SparkSession, schema: StructType): DataFrame = {
    if (filePath == null || filePath.trim() == "") {
      throw new RuntimeException("filePath is null or blank")
    } else if (spark == null) {
      throw new RuntimeException("spark session is null")
    } else {
      //Please note Spark expects each line to be a separate JSON object, so it will fail if you’ll try to load a pretty formatted JSON file
      //first convert into single line JSON Object
      val rawJson = spark.sparkContext.wholeTextFiles(filePath).map(tuple => tuple._2.replace("\n", "").trim)
      var jsonDF = spark.read
        .schema(schema)
        .json(rawJson)
      jsonDF
    }
  }

  def saveToCSV[T](filePath: String, ds: Dataset[T]) = {
    if (filePath == null || filePath.trim() == "") {
      throw new RuntimeException("filePath is null or blank")
    } else {
      ds.write.mode("Overwrite")
        .format("csv")
        .option("header", true)
        .option("delimiter", ",")
        .save(filePath)
    }
  }

  case class start_day_position(Instrument: Option[String], Account: Option[String], AccountType: Option[String], Quantity: Option[String])
  case class input_transaction(TransactionId: Option[Int], Instrument: Option[String], TransactionType: Option[String], TransactionQuantity: Option[String])
  case class end_day_position(Instrument: Option[String], Account: Option[String], AccountType: Option[String], Quantity: Option[String], Delta: Option[String])
}