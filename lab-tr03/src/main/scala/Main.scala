import org.apache.spark.sql.SparkSession

import scala.util.matching.Regex

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello, I'm TR03!")
      
    val keyValPattern: Regex = "^([\\w]+)=([\\w=\\*\\.\\/ ]+)$".r;
    val map = args.map(arg => {
          val keyVal = keyValPattern.findPrefixMatchOf(arg).getOrElse(throw new RuntimeException("not valid arg"))
          (keyVal.group(1), keyVal.group(2))
    }).toMap

    println(s"input data: $map")

    val inpath = map.get("inpath").getOrElse(throw new RuntimeException("not set variable 'inpath'"))
    val mask = map.get("mask").getOrElse(throw new RuntimeException("not set variable 'mask'"))
    val outpath = map.get("outpath").getOrElse(throw new RuntimeException("not set variable 'outpath'"))


    fileTransformation(inpath, mask, outpath)
  }

  def fileTransformation(inpath: String, mask: String, outpath: String) = {
    val spark = SparkSession
      .builder()
      .appName("Java Spark SQL TR03")
      .config("spark.master", "local")
      .getOrCreate();


    import spark.sqlContext.implicits._

    val columns = Seq("filename", "content")
    val filesRead = spark.sparkContext.wholeTextFiles(inpath + "/" + mask)
    val df = filesRead.toDF(columns: _*)
    df.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(outpath)

    df.printSchema()
    df.show()
    spark.stop()
  }
}