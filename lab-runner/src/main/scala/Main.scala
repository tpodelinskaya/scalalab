import com.google.gson.{JsonElement, JsonObject, JsonParser}

import java.io.{File, FileReader}

object Main {

  //It would be worth connecting a logger instead of print
  def main(args: Array[String]): Unit = {
    println("Hello! I'm Runner!")


    val (confDirKey, sparkKey) = ("confDir", "spark")

    def getOption(list: List[String]): Map[String, String] = {
      def nextOption(list: List[String], map: Map[String, String]): Map[String, String] = {
        list match {
          case Nil => map
          case "--spark" :: value :: tail => nextOption(tail, Map(sparkKey -> value) ++ map)
          case "--confDir" :: tail => nextOption(tail.tail, Map(confDirKey -> tail.head) ++ map)
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

    println("Input configs" + confDir.mkString(" "))

    confDir.foreach(runJarOnConfig(sparkSubmit)(_))
  }

  def runJarOnConfig(sparkSubmit: String)(pathToJar: String): Unit = {
    import scala.sys.process._

    implicit def jsonElementGetString(json: JsonElement) = json.getAsString

    implicit def jsonElementToJsonObject(json: JsonElement) = json.getAsJsonObject

    def jsonParameterToString(jsonParameters: JsonObject, parameterFormat: (String, String) => String): String = {
      jsonParameters.keySet().toArray.map(x => {
        parameterFormat(x.toString, jsonParameters.get(x.toString).getAsString)
      }).mkString(" ")
    }

    val confJson = new JsonParser().parse(new FileReader(pathToJar)).getAsJsonObject

    val classMain: String = confJson.get("class")
    val appName = confJson.get("app-name").toString //right, if parameter type string
    val jarFile: String = confJson.get("jar-file-path")

    val sparkParams = jsonParameterToString(
      confJson.get("spark-params"),
      (x1: String, x2: String) => s"--$x1 $x2"
    )
    val trParams = jsonParameterToString(
      confJson.get("tr-params"),
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
