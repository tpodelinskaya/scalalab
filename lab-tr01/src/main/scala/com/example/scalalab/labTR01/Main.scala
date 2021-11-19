package com.example.scalalab.labTR01

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Main extends Constants with Utils {

  def main(args: Array[String]): Unit = {

    val params: Map[String, String] = args
        .map(_.split("="))
        .map(t => (t.head, t.tail.mkString("=")))
        .toMap

    val path = getOrThrowErr(params, CSV_PATH)

    val spark: SparkSession = SparkSession
      .builder
      .getOrCreate()

    val reader = new ExternalReader(spark)

    val csv = reader.readCSV(path)

    val fioFromDB = reader.selectFromDB(params, getFileContent(PERSONAL_QUERY))
    val balanceFromDB = reader.selectFromDB(params, getFileContent(BALANCE_QUERY))
      .withColumn(ACT_BALANCE, col(ACT_BALANCE).cast("decimal(25,2)"))

    val leftJoinDF = fioFromDB
      .join(csv, Seq(NAME), LEFT)
      .join(balanceFromDB, Seq(ID), LEFT)

    val windowSpec  = Window.partitionBy(LEDGER)

    val sum_balancedf:DataFrame = leftJoinDF
      .select(LEDGER, ACT_BALANCE)
      .withColumn(SUM_BALANCE, sum(ACT_BALANCE).over(windowSpec))
      .drop(ACT_BALANCE)
      .distinct()


    val select_df = leftJoinDF
      .join(sum_balancedf, Seq(LEDGER), INNER)
      .select(
        col(NAME),
        col(CN),
        col(AC_NUMBER),
        col(LEDGER),
        col(ID),
        col(ACT_BALANCE),
        col(SUM_BALANCE)
    )

    val resDF = makeResultWithUDF(select_df)


    val writePath = getOrThrowErr(params, WR_PATH)

    resDF
      .coalesce(1)
      .sort(NAME)
      .write
      .format(TEXT)
      .mode(SaveMode.Overwrite)
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
        col(NAME),
        col(CN),
        col(AC_NUMBER),
        col(LEDGER),
        col(ACT_BALANCE),
        col(SUM_BALANCE)

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
      .toDF(RESULT)
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
        RESULT,
        getResUDF(col(NAME),
                  col(CN),
                  col(AC_NUMBER),
                  col(LEDGER),
                  col(ACT_BALANCE),
                  col(SUM_BALANCE)
                )
      )
      .select(RESULT)

  }

}
