package com.example.scalalab.labTR01

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import Args.extract
import Utils.{getOrThrowErr, getResources}
import org.apache.spark.sql.expressions.Window

object Main {

  def main(args: Array[String]): Unit = {

    val params: Map[String, String] = extract(args)

    //Нет обработки, если у нас во входных параметрах нет path_csv
    //Константы лучше выность в переменные, что при их изменении править в одном месте
    val path = getOrThrowErr(params, "path_csv")

    //Как запускается spark - локально или на сервере тоже вынести в настройки
    //Наименование приложения вынести в настройки
    //И то и то будет в конфигурационном файле json, а значит, может быть подставленно раннером
    //И уже забрано трансформацией внутри себя
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("SparklabTR01")
      .getOrCreate()

    val reader = new ExternalReader(spark)

    val csv = reader.readCSV(path)
    //Не уверена, что при запуске через раннер, файлы query.sql и balance.sql автоматически найдутся, надо проверить
    //Зачем нам сортировка ORDER BY name в query.sql ?
    val fioFromDB = reader.selectFromDB(params, getResources("query.sql"))
    val balanceFromDB = reader
      .selectFromDB(params, getResources("balance.sql"))
      .withColumn("act_balance", col("act_balance").cast("decimal(25,2)"))

    val leftJoinDF = fioFromDB
      .join(csv, Seq("name"), "left")
      .join(balanceFromDB, Seq("id"), "left")

    val windowSpec  = Window.partitionBy("ledger")

    val sum_balancedf:DataFrame = leftJoinDF
      .select("ledger", "act_balance")
      .withColumn("sum_balance",sum("act_balance").over(windowSpec))
      .drop("act_balance")
      .distinct()


    val select_df = leftJoinDF
      .join(sum_balancedf, Seq("ledger"), "inner")
      .select(
        col("name"),
        col("CN"),
        col("account_number"),
        col("ledger"),
        col("id"),
        col("act_balance"),
        col("sum_balance")
    )
    //Я все-таки голосую за spark.sql :) Но это не принципиально.
    //Какая задумка у makeResultWithUDF и чем на не подходит write в DataFrame ?
    val resDF = makeResultWithUDF(select_df)


    val writePath = getOrThrowErr(params, "write_path")

    resDF
      .coalesce(1)
      .sort("name")
      .write
      .format("text")
      .mode("overwrite")
      .save(writePath)

    spark.stop()
  }

  def makeResultWithMapPartition(df: DataFrame) = {

    def combine(fio: String, cn: String, ac: String, ledger: String, actBalance: String, sumBalance: String): String =
         s"""ФИО: $fio
         |Номер договора: $cn
         |Лицевой счет клиента: $ac
         |Актуальный баланс по лицевому счету: $actBalance
         |Суммарный баланс по балансовому счету $ledger: $sumBalance
         |""".stripMargin

    import df.sparkSession.implicits._

    df.select(
        col("name"),
        col("CN"),
        col("account_number"),
        col("ledger"),
        col("act_balance"),
        col("sum_balance")

      )
      .mapPartitions(iterator => {
        iterator.map(row => {
          combine(
            row.getString(0),
            row.getString(1),
            row.getString(2),
            row.getString(3),
            row.getString(4),
            row.getString(5)
          )
        })

      })
      .toDF("result")
  }

  def makeResultWithUDF(df: DataFrame) = {
    val getResUDF =
      udf((fio: String, cn: String, ac: String, ledger: String, actBalance: String, sumBalance: String) => {
        s"""ФИО: $fio
           |Номер договора: $cn
           |Лицевой счет клиента: $ac
           |Актуальный баланс по лицевому счету: $actBalance
           |Суммарный баланс по балансовому счету $ledger: $sumBalance
           |""".stripMargin
      })

    df.withColumn(
        "result",
        getResUDF(col("name"),
                  col("CN"),
                  col("account_number"),
                  col("ledger"),
                  col("act_balance"),
                  col("sum_balance")
                )
      )
      .select("result")

  }

}
