import sbt._

object Dependencies {

  val scalaCompat: String = "2.12.12"

  private object Version {
    val spark              = "2.4.8"
    val sparkSQL           = "2.4.8"
  }

  object Spark {

    val Core =  "org.apache.spark" %% "spark-core" % Version.spark % "provided"
    val Sql = "org.apache.spark" %% "spark-sql" % Version.sparkSQL % "provided"


  }
}
