import sbt._

object Dependencies {

  val scalaCompat: String = "2.12.12"
  val scalaTestVersion = "3.2.10"

  private object Version {
    val spark = "2.4.8"
    val sparkSql = "2.4.8"

  }

  object Spark {

    val Core = "org.apache.spark" %% "spark-core" % Version.spark % "provided"
    val Sql = "org.apache.spark" %% "spark-sql" % Version.sparkSql % "provided"

  }

  object Other {
    val gson = "com.google.code.gson" % "gson" % "2.8.8"
    val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % Test
    val commonsCli = "commons-cli" % "commons-cli" % "1.5.0"
  }

}
