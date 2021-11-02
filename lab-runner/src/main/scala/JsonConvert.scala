import com.google.gson.JsonObject

object JsonConvert {
  private def jsonParameterToString(jsonParameters: JsonObject, parameterFormat: (String, String) => String): String = {

    def getParameter(x: String): String = {
      val argParam = jsonParameters.get(x)

      if (argParam.isJsonPrimitive && argParam.getAsJsonPrimitive.isString) {
        argParam.getAsString
      } else if (argParam.isJsonArray) {
        val jsonArray = argParam.getAsJsonArray
        (for (i <- 0 until jsonArray.size()) yield {jsonArray.get(i).getAsString}).mkString(" ")
      } else {
        throw new RuntimeException("Error, jsonElement = " + argParam.toString)
      }
    }

    jsonParameters.keySet().toArray.map(x => {
      parameterFormat(x.toString, getParameter(x.toString))
    }).mkString(" ")
  }

  def createCommand(sparkSubmit: String)(confJson: JsonObject): String = {
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
