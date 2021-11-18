package com.example.scalalab.labTR01

import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ExternalReader(spark: SparkSession) extends Utils {

  def readCSV(path: String): DataFrame = {
    val schema = new StructType()
      .add("name", StringType, false)
      .add("birth_date", DateType, false)
      .add("inn", StringType, false)
      .add("passport", StringType, false)
      .add("branch", StringType, false)
      .add("CN", StringType, false)
      .add("open_date", DateType, false)
      .add("close_date", DateType, true)
      .add("col9", DoubleType, true)
      .add("col10", StringType, true)

    val df = spark.read
      .options(
        Map(
          "delimiter" -> ";",
          "header" -> "false",
          "dateFormat" -> "dd.MM.yyyy"
        )
      )
      .schema(schema)
      .csv(path)

    df
  }

  //Нет обрабоки ошибок, допустим, что у нас база недоступна
  //Вывалится эксепшен
  def selectFromDB(params: Map[String, String], query: String): DataFrame = {

    spark.read
      .format("jdbc")
      .option("url",
        getOrThrowErr(params, "jdbc_uri")
      )
      .option("query", query)
      .option(
        "user",
        getOrThrowErr(params, "login")
      )
      .option(
        "password",
        getOrThrowErr(params, "password")
      )
      .load()
  }

}
