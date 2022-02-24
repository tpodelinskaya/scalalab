import Args.{Args, formatArgs, getArg}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, lit}

import java.time.LocalTime
import java.time.format.DateTimeFormatter

object Main {

  def main(args: Array[String]): Unit = {
    println("Hello, I'm TR06!")

    val argsMap: Args = formatArgs(args)

    println(s"Input data: $argsMap")

    val inpath = getArg(argsMap, "inpath")
    val mask = getArg(argsMap, "mask")
    val outpath = getArg(argsMap, "outpath")


    fileTransformation(inpath, mask, outpath)
  }


  def fileTransformation(inpath: String, mask: String, outpath: String): Unit = {

    val spark = SparkSession
      .builder()
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")


    val formatter = DateTimeFormatter.ofPattern("HH:mm:ss")

    val maxSizeStr = 5 //for test

    val df = spark.read
      .option("header", true)
      //todo:   .option("inferSchema", true)
      .format("com.databricks.spark.csv")
      .load(inpath + mask)
      .withColumn("load_data", current_date())
      .withColumn("load_time", lit(formatter.format(LocalTime.now())))

    val stringColumn = df.schema.fields.filter(_.dataType.typeName == "string")
      .map(x => col(x.name))

    val arrRDD: Array[RDD[String]] = stringColumn.map(df.select(_)).map(_.rdd.map(r => r(0)).map(_.toString))

    val x: Array[RDD[String]] = arrRDD.map(z => z.map(f =>
      if (f.length > maxSizeStr) "Truncated: " + f.substring(0, maxSizeStr)
      else f)
    )

    x.map(_.toDF()).reduce((v1, v2) => v1.join(v2)).show()

  }
}
