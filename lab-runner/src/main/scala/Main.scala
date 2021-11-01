import com.google.gson.JsonParser

import java.io.{File, FileReader}

object Main extends ArgsParser with JsonConvert {

  def main(args: Array[String]): Unit = {
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
