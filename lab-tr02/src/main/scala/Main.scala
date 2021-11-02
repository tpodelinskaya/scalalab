import org.apache.spark.sql.SparkSession


object Main {
  def main(args: Array[String]): Unit = {
    println("Hello, I'm TR02!")

    val inpath = "E:\\Учёба!\\Scala\\new_project\\scalalab\\lab-tr02\\src\\main\\scala\\Simple1.html"

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
      .flatMap(lines => lines.split("  "))
      .filter(word => (word.startsWith("<") && word.endsWith(">")))
    val counts = words
      .map(w => (w, 1))
      .reduceByKey(_ + _)

    val columns = Seq("Tag", "Count")

    val df = counts.toDF(columns: _*)
    //df.coalesce(1).write.option("header", true).mode("append").csv("file:///C:/Users/Валерик/Downloads/htmls")

    df.show()
    df.printSchema()

    spark.stop()


  }
}
