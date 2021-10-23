import org.apache.spark.sql.SparkSession

object TestFilter {
  def main(args: Array[String]): Unit = {
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
