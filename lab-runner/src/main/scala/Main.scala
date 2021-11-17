import JsonConvert.createCommand
import com.google.gson.JsonParser

import java.io.{File, FileReader}
import scala.sys.process._

object Main extends ArgsParser {
  validation()

  val formatFile = ".json"

  val configs = new File(confDir).listFiles(
    (file: File) => file.exists() && file.getName.endsWith(formatFile)
  ).map(_.toPath.toString)

  println("Hello! I'm Runner!")
  println("Input configs: " + configs.mkString("[", ", ", "]"))

  configs.foreach(conf => {
    try {
      val confJson = JsonParser.parseReader(new FileReader(conf)).getAsJsonObject
      val commandRun = createCommand(spark)(confJson)
      println(s"CommandRun = $commandRun")
      val result = commandRun.!
      reactToAnErrorRunProgram(result != 0, s"Error while executing the program, result = $result, conf = $conf")
    } catch {
      case _: com.google.gson.JsonSyntaxException =>
        reactToAnErrorFormat(true, s"Not valid json, conf = $conf")
      case _: ExceptionParseJson =>
        reactToAnErrorFormat(true, s"Not valid data in json, conf = $conf")
    }
  }
  )

}
