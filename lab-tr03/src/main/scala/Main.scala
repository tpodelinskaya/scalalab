import Args.{Args, formatArgs, getArg}
import org.apache.spark.sql.SparkSession

import scala.util.matching.Regex

object Main {

  def main(args: Array[String]): Unit = {
    println("Hello, I'm TR03!")

    val argsMap: Args = formatArgs(args)

    println(s"Input data: $argsMap")

    //Где обработка выплюнутых эксепшенов для пользователя?

    val inpath = getArg(argsMap, "inpath")
    val mask = getArg(argsMap, "mask")
    val outpath = getArg(argsMap, "outpath")


    fileTransformation(inpath, mask, outpath)
  }



  def fileTransformation(inpath: String, mask: String, outpath: String): Unit = {

    //Как запускается spark - локально или на сервере тоже вынести в настройки
    //Наименование приложения вынести в настройки
    //И то и то будет в конфигурационном файле json, а значит, может быть подставленно раннером
    //И уже забрано трансформацией внутри себя
    val spark = SparkSession
    .builder()
    .appName("Java Spark SQL TR03")
    .config("spark.master", "local")
    .getOrCreate()

    import spark.sqlContext.implicits._

    val filesRead = spark.sparkContext.wholeTextFiles(s"$inpath${System.getProperty("file.separator")}$mask")
    val columns = Seq("filename", "content")
    val filesDF = filesRead.toDF(columns: _*)
    filesDF.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(outpath)

    filesDF.printSchema()
    filesDF.show()

    spark.stop()
  }
}
