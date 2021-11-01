import com.google.gson.JsonObject

trait JsonConvert {
  def jsonParameterToString(jsonParameters: JsonObject, parameterFormat: (String, String) => String): String = {
    jsonParameters.keySet().toArray.map(x => {
      parameterFormat(x.toString, jsonParameters.get(x.toString).getAsString)
    }).mkString(" ")
  }
}
