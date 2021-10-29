import com.google.gson.{JsonObject, JsonParser}

import java.io.{File, FileReader}
import scala.sys.exit

object Main {

  def main(args: Array[String]): Unit = {
    println("Hello! I'm Runner!")

    val (confDirKey, sparkKey) = ("confDir", "spark")

    val help =
      """
        |This program launches other programs, based on their configuration, western in sjon format
        |====================================================================================
        |--help     - parameter for displaying this help
        |--confDir  - parameter pointing to the configuration directory
        |--spark    - specifies the path to spark-submit, can be omitted
        |""".stripMargin

    def getOption(list: List[String]): Map[String, String] = {
      def nextOption(list: List[String], map: Map[String, String]): Map[String, String] = {
        list match {
          case Nil => map
          case "--spark" :: value :: tail => nextOption(tail, Map(sparkKey -> value) ++ map)
          case "--confDir" :: value :: tail => nextOption(tail, Map(confDirKey -> value) ++ map)
          case "--help" :: tail => println(help); exit(0)
          case _ => throw new RuntimeException("Not valid optional")
        }
      }
      nextOption(list, Map())
    }

    val mapOption = getOption(args.toList)

    val sparkSubmit = mapOption.get(sparkKey).getOrElse("spark-submit")
    val confDir = new File(mapOption.get(confDirKey).getOrElse(throw new RuntimeException("Key confDir not found"))).listFiles(
      (file: File) => file.exists() && file.getName.endsWith(".json")
    ).map(_.toPath.toString)

    println("Input configs: " + confDir.mkString("[", ",", "]"))

    confDir.foreach(runJarOnConfig(sparkSubmit)(_))
  }

  def runJarOnConfig(sparkSubmit: String)(pathToJar: String): Unit = {
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
