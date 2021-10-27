import com.google.gson.{JsonElement, JsonParser}

import java.io.FileReader
import scala.sys.process._

object Main {

  def main(args: Array[String]): Unit = {
    println("Hello! I'm Runner!")

    implicit def anyRefToString(any: AnyRef) = any.toString

    implicit def jsonElementGetString(json: JsonElement) = json.getAsString


    val jsonConf = new JsonParser().parse(new FileReader(args(0))).getAsJsonObject
    val sparkSubmit = if (args.length == 2) args(1) else "spark-submit"

    val classMain: String = jsonConf.get("class")
    val appName = jsonConf.get("app-name").toString //right
    val jarFile: String = jsonConf.get("jar-file-path")

    val sparkParams = jsonConf.get("spark-params").getAsJsonObject
    val master = sparkParams.get("master") //Call error
    val deployMode = sparkParams.get("deploy-mode") //Call error
    val executorCores: String = sparkParams.get("executor-cores")
    val executorMemory: String = sparkParams.get("executor-memory")
    val driverMemory: String = sparkParams.get("driver-memory")

    val trParamsJson = jsonConf.get("tr-params").getAsJsonObject
    val trParams = trParamsJson.keySet().toArray().map(x => {
      s"$x=${trParamsJson.get(x).getAsString}"
    }).mkString(" ")


    val command =
      f"""$sparkSubmit
          --name $appName
         --executor-memory $executorMemory
         --driver-memory $driverMemory
         --executor-cores $executorCores
         --class $classMain
         ${jarFile} $trParams""".stripMargin

    println(command)

    (command).!
  }
}
