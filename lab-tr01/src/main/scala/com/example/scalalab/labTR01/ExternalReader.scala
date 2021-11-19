package com.example.scalalab.labTR01

import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{FileNotFoundException, IOException}

class ExternalReader(spark: SparkSession) extends Utils with Constants {

  def readCSV(path: String): DataFrame = {
    val schema = new StructType()
      .add(NAME, StringType, false)
      .add(BIRTHD, DateType, false)
      .add(INN, StringType, false)
      .add(PASSPORT, StringType, false)
      .add(BRANCH, StringType, false)
      .add(CN, StringType, false)
      .add(OPEND, DateType, false)
      .add(CLOSED, DateType, true)
      .add(COL9, DoubleType, true)
      .add(COL10, StringType, true)

    try {
      spark.read
        .options(
          Map(
            "delimiter" -> ";",
            "header" -> "false",
            "dateFormat" -> "dd.MM.yyyy"
          )
        )
        .schema(schema)
        .csv(path)
    } catch {
      case e: FileNotFoundException => throw new LabTr01Exception("CSV file not found ", e.getCause)
      case e: Exception => throw  new LabTr01Exception("CSV file read error ", e.getCause)
    }
  }

  def selectFromDB(params: Map[String, String], query: String): DataFrame = {
    try {
      spark.read
        .format("jdbc")
        .option(URL,
          getOrThrowErr(params, JDBC_URL)
        )
        .option(QUERY, query)
        .option(
          USER,
          getOrThrowErr(params, LOGIN)
        )
        .option(
          PASSWORD,
          getOrThrowErr(params, PASSWORD)
        )
        .load()
    } catch {
      case e: IOException => throw new LabTr01Exception("Error db access: ", e.getCause)
      case e: Exception => throw new LabTr01Exception("Database select error: ", e.getCause)
    }
  }

  class LabTr01Exception(msg:String, cause: Throwable) extends Exception(msg, cause)
}
