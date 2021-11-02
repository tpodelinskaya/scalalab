import ArgsParser.reactToAnError
import JsonConvert.createCommand
import com.google.gson.JsonParser

import java.io.FileReader

object Main {

  def main(args: Array[String]): Unit = {
    val argsParser = new ArgsParser(args)

    println("Hello! I'm Runner!")
    println("Input args: " + argsParser.mapOption.mkString("[", ", ", "]"))
    println("Input configs: " + argsParser.confDir.mkString("[", ", ", "]"))

    argsParser.confDir.foreach(conf => {
      try {
        val confJson = JsonParser.parseReader(new FileReader(conf)).getAsJsonObject
        val commandRun = createCommand(argsParser.sparkSubmit)(confJson)
        println(s"CommandRun = $commandRun")
        val result = runCommand(commandRun)
        reactToAnError(argsParser.stopOnRunError, result != 0, s"Error while executing the program, result = $result, conf = $conf")
      } catch {
        case _: com.google.gson.JsonSyntaxException =>
          reactToAnError(argsParser.stopOnErrorFormat, msg = s"Not valid json, conf = $conf")
        case _: ExceptionParseJson =>
          reactToAnError(argsParser.stopOnErrorFormat, msg = s"Not valid data in json, conf = $conf")
      }
    }
    )
  }

  def runCommand(command: String): Int = {
    import scala.sys.process._

    command.!
  }
}
