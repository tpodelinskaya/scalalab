import JsonConvert.createCommand
import com.google.gson.JsonParser

import java.io.{File, FileReader}

object Main {
  def main(args: Array[String]): Unit = {
    val formatFile = ".json"

    val pArgs = new ArgsParser(args)

    val configs = new File(pArgs.confDir).listFiles(
      (file: File) => file.exists() && file.getName.endsWith(formatFile)
    ).map(_.toPath.toString)

    println("Hello! I'm Runner!")
    println("Input configs: " + configs.mkString("[", ", ", "]"))

    configs.foreach(conf => {
      try {
        val confJson = JsonParser.parseReader(new FileReader(conf)).getAsJsonObject
        val commandRun = createCommand(pArgs.spark())(confJson)
        println(s"CommandRun = $commandRun")
        val result = runCommand(commandRun)
        pArgs.reactToAnErrorRunProgram(result != 0, s"Error while executing the program, result = $result, conf = $conf")
      } catch {
        case _: com.google.gson.JsonSyntaxException =>
          pArgs.reactToAnErrorFormat(true, s"Not valid json, conf = $conf")
        case _: ExceptionParseJson =>
          pArgs.reactToAnErrorFormat(true, s"Not valid data in json, conf = $conf")
      }
    }
    )
  }

  def runCommand(command: String): Int = {
    import scala.sys.process._

    command.!
  }
}
