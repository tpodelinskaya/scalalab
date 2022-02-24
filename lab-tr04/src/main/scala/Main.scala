import Args.{Args, formatArgs, getArg}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.matching.Regex

object Main {

  def main(args: Array[String]): Unit = {
    println("Hello, I'm TR04!")

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

    val allFile: DataFrame = spark.sparkContext.textFile(inpath + mask)
      .map(_.split(" "))
      .toDF()

    val df = allFile.select(explode($"value").as("word"))
      .groupBy("word")
      .count()
      .as("cnt")

    df.show()

    df.coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", false)
      .save(outpath)


    spark.stop()
  }
}
