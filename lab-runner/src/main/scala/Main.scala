import com.google.gson.JsonParser

import java.io.FileReader
import sys.process._

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello! I'm Runner!")

    val jsonConf = new JsonParser().parse(new FileReader(args(0))).getAsJsonObject
    println("input config: " + jsonConf)
    val sparkCommand = (f"/home/redcrab/Public/spark/bin/spark-submit --class ${jsonConf.get("class").getAsString} " +
      f"${jsonConf.get("jar-file-path").getAsString} " +
      f"inpath=/home/redcrab/test=dir mask=*.txt outpath=./out_path").!

    sparkCommand
  }
}
