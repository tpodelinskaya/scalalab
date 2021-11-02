import com.google.gson.{JsonObject, JsonParser}

import java.io.FileReader

object JsonConvert {
  private def jsonParameterToString(jsonParameters: JsonObject, parameterFormat: (String, String) => String): String = {

    jsonParameters.keySet().toArray.map(x => {
      parameterFormat(x.toString, jsonParameters.get(x.toString).getAsString)
    }).mkString(" ")
  }

  def createCommand(sparkSubmit: String)(pathToJar: String): String = {
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

    command
  }
}
