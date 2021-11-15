package com.example.scalalab.labTR01

object Utils {
  def getResources(name: String): String = {
    import scala.io.Source

    val source = Source.fromResource(name)
    val query = source.mkString
    source.close()

    query
  }

  def getStringException(value: String):String =
    s"Param $value not found"

  def getOrThrowErr(map: Map[String, String], key:String): String =
    map.get(key) match {
      case None => throw new IllegalArgumentException(getStringException(key))
      case Some(value) => value
    }

}
