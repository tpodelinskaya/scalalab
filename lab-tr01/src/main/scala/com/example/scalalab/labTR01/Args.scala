package com.example.scalalab.labTR01

object Args {

  def extract(args: Array[String]): Map[String, String] = {
    args
      .map(_.split("="))
      .map(t => (t.head, t.tail.mkString("=")))
      .toMap
  }

}
