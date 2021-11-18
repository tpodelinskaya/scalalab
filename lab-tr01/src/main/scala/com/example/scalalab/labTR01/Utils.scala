package com.example.scalalab.labTR01

import scala.util.{Failure, Success, Try}

trait Utils {

  def getFileContent(name: String): String = Try {
    import scala.io.Source
    val source = Source.fromResource(name)
    val query = source.mkString
    source.close()
    query
  } match {
    case Success(value) => value
    case Failure(exception) => throw exception
  }

  def getStringException(value: String):String =
    s"Param $value not found"

  def getOrThrowErr(map: Map[String, String], key:String): String =
    map.get(key) match {
      case None => throw new IllegalArgumentException(getStringException(key))
      case Some(value) => value
    }

}
