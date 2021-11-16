package com.example.scalalab.labTR01

object Args {
  //Если используется единоразово, то нет смысла выносить в отдельный метод и объект
  def extract(args: Array[String]): Map[String, String] = {
    args
      .map(_.split("="))
      .map(t => (t.head, t.tail.mkString("=")))
      .toMap
  }

}
