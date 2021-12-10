import org.apache.spark.sql.SparkSession

import scala.util.matching.Regex


object Main {

  type Args = Map[String, String]

  def main(args: Array[String]): Unit = {
    println("Hello, I'm TR02!")

    val argsMap: Args = formatArgs(args)

    println(s"Input data: $argsMap")

    val inpath = getArg(argsMap, "inpath")
    val outpath = getArg(argsMap, "outpath")

    transformation(inpath, outpath)
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

  def transformation(inpath: String, outpath: String) = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val sc = spark.sparkContext

    val readFile = sc.textFile(inpath)

    val keyValP: Regex = "<[^>]*>".r

    val tags = readFile
      .flatMap(lines => keyValP.findAllIn(lines))
      .map(items => (items, 1))
      .reduceByKey(_ + _)

    val columns = Seq("{Tag}", "{Count}")

    val df = tags.toDF(columns: _*)

    df
      .coalesce(1)
      .write.option("header", true)
      .mode("overwrite")
      .csv(outpath)

    df.show()
    df.printSchema()

    spark.stop()
  }
}
