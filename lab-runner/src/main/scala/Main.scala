import com.google.gson.{JsonObject, JsonParser}

import java.io.{File, FileReader}
import scala.annotation.tailrec
import scala.sys.exit

object Main {

  def main(args: Array[String]): Unit = {

    def reactToAnError(stopProgram: Boolean = true)(error: => Boolean, msg: String): Unit = {
      if (error) {
        if (stopProgram) {
          throw new RuntimeException(msg)
        } else {
          println(s"!!! $msg")
        }
      }
    }

    val (confDirKey, sparkKey, stopOnErrorFormatKey, stopOnRunErrorKey) = ("confDir", "spark", "stopOnErrorFormat", "stopOnRunError")

    val help =
      """
        |This program launches other programs, based on their configuration, western in json format
        |====================================================================================
        |--help                           - parameter for displaying this help
        |--confDir [path]                 - parameter pointing to the configuration directory
        |--spark [path]                   - specifies the path to spark-submit, can be omitted
        |--stopOnErrorFormat [true/false] - pause the program on erroneous input? Default true
        |--stopOnRunError [true/false]    - stop the program if an error occurs as a result of startup
        |""".stripMargin

    type MapOptional = Map[String, String]

    @tailrec
    def nextOption(list: List[String], map: MapOptional = Map()): MapOptional = {
      list match {
        case Nil => map
        case "--spark" :: value :: tail => nextOption(tail, Map(sparkKey -> value) ++ map)
        case "--confDir" :: value :: tail => nextOption(tail, Map(confDirKey -> value) ++ map)
        case "--stopOnErrorFormat" :: value :: tail => nextOption(tail, Map(stopOnErrorFormatKey -> value) ++ map)
        case "--stopOnRunError" :: value :: tail => nextOption(tail, Map(stopOnRunErrorKey -> value) ++ map)
        case "--help" :: _ => println(help); exit(0)
        case _ => throw new RuntimeException("Not valid optional")
      }
    }


    val mapOption = nextOption(args.toList)
    val stopOnErrorFormat = mapOption.getOrElse(stopOnErrorFormatKey, true).toString.toBoolean
    val stopOnRunError = mapOption.getOrElse(stopOnRunErrorKey, true).toString.toBoolean

    if (mapOption.contains(sparkKey)) {
      reactToAnError()(!new File(mapOption(sparkKey)).canExecute, "Not found spark-submit")
    }
    reactToAnError()(!mapOption.contains(confDirKey), "Key confDir not found")
    reactToAnError()(!new File(mapOption(confDirKey)).isDirectory, "Not found dir with configs")

    val sparkSubmit = mapOption.getOrElse(sparkKey, "spark-submit")
    val confDir = new File(mapOption(confDirKey)).listFiles(
        (file: File) => file.exists() && file.getName.endsWith(".json")
    ).map(_.toPath.toString)

    println("Hello! I'm Runner!")
    println("Input args: " + mapOption.mkString("[", ", ", "]"))
    println("Input configs: " + confDir.mkString("[", ", ", "]"))

    confDir.foreach(conf => {
      try {
        val result = runJarOnConfig(sparkSubmit)(conf)
        reactToAnError(stopOnRunError)(result != 0, s"Error while executing the program, result = $result, conf = $conf")
      } catch {
        case _: com.google.gson.JsonSyntaxException =>
          reactToAnError(stopOnErrorFormat)(error = true, s"Not valid json, conf = $conf")
        case _: java.lang.NullPointerException =>
          reactToAnError(stopOnErrorFormat)(error = true, s"Not valid data in json, conf = $conf")
      }
    }
    )
  }

  def runJarOnConfig(sparkSubmit: String)(pathToJar: String): Int = {
    import scala.sys.process._

    def jsonParameterToString(jsonParameters: JsonObject, parameterFormat: (String, String) => String): String = {
      jsonParameters.keySet().toArray.map(x => {
        parameterFormat(x.toString, jsonParameters.get(x.toString).getAsString)
      }).mkString(" ")
    }

    val confJson = new JsonParser().parse(new FileReader(pathToJar)).getAsJsonObject

    val classMain = confJson.get("class").getAsString
    val appName = "\"" + confJson.get("app-name").getAsString + "\""
    val jarFile = confJson.get("jar-file-path").getAsString

    val sparkParams = jsonParameterToString(
      confJson.getAsJsonObject("spark-params"),
      (x1: String, x2: String) => s"--$x1 $x2"
    )
    val trParams = jsonParameterToString(
      confJson.getAsJsonObject("tr-params"),
      (x1: String, x2: String) => s"$x1=$x2"
    )

    val command =
      f"""$sparkSubmit
         | --name $appName
         | $sparkParams
         | --class $classMain
         | $jarFile $trParams""".stripMargin.replace("\n", " ")

    println(s"CommandRun = $command")

    command.!
  }
}
