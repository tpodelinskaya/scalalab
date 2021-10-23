import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello, I'm TR02!")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("TestFilter")
      .getOrCreate()
    val sc = spark.sparkContext
    val rdd = sc.textFile("src/main/scala/Text.txt")
    val filterRDD = rdd.filter(x => x.startsWith("<") && x.endsWith(">"))
    val count = filterRDD.count();
    println(count);
  }
}
