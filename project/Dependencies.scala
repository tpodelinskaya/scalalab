import sbt._

object Dependencies {

  val scalaCompat: String = "2.12.12"

  private object Version {
    val spark              = "3.2.0"
    val sparkSql           = "3.2.0"

  }

  object Spark {

    val Core =  "org.apache.spark" %% "spark-core" % Version.spark % "provided"
    val Sql =   "org.apache.spark" %% "spark-sql" % Version.sparkSql % "provided"

  }
}
