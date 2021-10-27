import org.apache.spark.sql.SparkSession


object Main {
  def main(args: Array[String]): Unit = {
    println("Hello, I'm TR02!")

    val inpath = "E:\\Учёба!\\Scala\\new_project\\scalalab\\lab-tr02\\src\\main\\scala\\Simple.html"

    transformation(inpath)
  }

  def transformation(inpath: String) = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark TR02")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val sc = spark.sparkContext

    val readFile = sc.textFile(inpath)

    val words = readFile
      .flatMap(lines => lines.split(" "))
      .filter(word => word.contains("<") && word.contains (">"))
    val counts = words
      .map(w => (w, 1))
      .reduceByKey(_ + _)

    val columns = Seq("Tag", "Count")

    val df = counts.toDF(columns: _*)

    df.printSchema()
    df.show()

    //df.write.option("header", true).mode("append").csv("E:\\Учёба!\\Scala\\new_project\\scalalab\\lab-tr02\\src\\main\\scala\\new.csv")

    spark.stop()


  }
}
