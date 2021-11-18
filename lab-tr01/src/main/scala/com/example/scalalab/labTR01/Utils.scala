package com.example.scalalab.labTR01

//Объект можно заменить на Trait
object Utils {
  //Я бы назвала getFileContent или что-то вроде
  //Нет обработки ошибок, надо добавить
  def getResources(name: String): String = {
    import scala.io.Source
    val source = Source.fromResource(name)
    val query = source.mkString
    source.close()
    query
  }

  //Почему функция, а не просто константа?
  def getStringException(value: String):String =
    s"Param $value not found"

  //Зачем этот метод, если ошибка потом в коде не обрабатывается
  //Можно в коде сразу использовать обработчик map.get(key) match
  //Зачем дополнительно генерировать эксепшен?
  def getOrThrowErr(map: Map[String, String], key:String): String =
    map.get(key) match {
      case None => throw new IllegalArgumentException(getStringException(key))
      case Some(value) => value
    }

}
