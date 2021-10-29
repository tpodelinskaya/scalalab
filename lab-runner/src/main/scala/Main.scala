import com.google.gson.{JsonObject, JsonParser}

import java.io.{File, FileReader}
import scala.sys.exit

object Main {

  def main(args: Array[String]): Unit = {
    println("Hello! I'm Runner!")

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

    def getOption(list: List[String]): Map[String, String] = {
      def nextOption(list: List[String], map: Map[String, String]): Map[String, String] = {
        list match {
          case Nil => map
          case "--spark" :: value :: tail => nextOption(tail, Map(sparkKey -> value) ++ map)
          case "--confDir" :: value :: tail => nextOption(tail, Map(confDirKey -> value) ++ map)
          case "--stopOnErrorFormat" :: value :: tail => nextOption(tail, Map(stopOnErrorFormatKey -> value) ++ map)
          case "--stopOnRunError" :: value :: tail => nextOption(tail, Map(stopOnRunErrorKey -> value) ++ map)
          case "--help" :: tail => println(help); exit(0)
          case _ => throw new RuntimeException("Not valid optional")
        }
      }
      nextOption(list, Map())
    }



    val mapOption = getOption(args.toList)

    val stopOnErrorFormat: Boolean = mapOption.get(stopOnErrorFormatKey).getOrElse(true).toString.toBoolean
    val stopOnRunError = mapOption.get(stopOnRunErrorKey).getOrElse(true).toString.toBoolean

    if (mapOption.contains(sparkKey)) {
      if (!new File(mapOption.get(sparkKey).get).canExecute) {
        throw new Exception("Not found spark-submit")
      }
    } else {
      throw new RuntimeException("Key confDir not found")
    }

    if (mapOption.contains(confDirKey)) {
      if (!new File(mapOption.get(confDirKey).get).isDirectory) {
        throw new RuntimeException("Not found dir with configs")
      }
    }


    val sparkSubmit = mapOption.get(sparkKey).getOrElse("spark-submit")
    val confDir = new File(mapOption.get(confDirKey).get).listFiles(
      (file: File) => file.exists() && file.getName.endsWith(".json")
    ).map(_.toPath.toString)


    println("Input configs: " + confDir.mkString("[", ", ", "]"))





    confDir.foreach(conf => {
      try {
        val result = runJarOnConfig(sparkSubmit)(conf)
        if (result != 0) {
          println(s"Error while executing the program, $result conf = $conf")
          if (stopOnRunError) {
            throw new RuntimeException(s"error in run app $conf, result = $result")
          }
        }
      } catch {
        case eSyntax: com.google.gson.JsonSyntaxException =>
          if (stopOnErrorFormat) {
            throw new RuntimeException("Not valid json, conf = " + conf)
          } else println("!!!Not valid json in " + conf)
        case eNull: java.lang.NullPointerException =>
          if (stopOnErrorFormat) {
            throw new RuntimeException("Not valid data in json, =" + conf)
          } else println("!!!Not valid data in json " + conf)
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

    val classMain: String = confJson.get("class").getAsString
    val appName = "\"" + confJson.get("app-name").getAsString + "\""
    val jarFile: String = confJson.get("jar-file-path").getAsString

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
         | ${jarFile} $trParams""".stripMargin.replace("\n", " ")

    println(command)

    command.!
  }
}
