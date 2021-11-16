import org.apache.spark.sql.SparkSession

import scala.util.matching.Regex

object Main {
  type Args = Map[String, String]

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

  def getArg(args: Args, nameArg: String): String = {
      args.getOrElse(nameArg, throw new RuntimeException(s"not set variable '$nameArg'"))
  }

  def formatArgs(argsArr:  Array[String]): Map[String, String] = {
    val keyValPattern: Regex = "^([\\w]+)=(.+)$".r
    argsArr.map(arg => {
      val keyVal = keyValPattern.findPrefixMatchOf(arg).getOrElse(throw new RuntimeException("not valid arg"))
      (keyVal.group(1), keyVal.group(2))
    }).toMap
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
