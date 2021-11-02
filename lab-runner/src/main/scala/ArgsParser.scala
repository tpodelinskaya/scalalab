import ArgsParser._

import java.io.File
import scala.annotation.tailrec
import scala.sys.exit

object ArgsParser {
  type MapOptional = Map[String, String]
  private val (confDirKey, sparkKey, stopOnErrorFormatKey, stopOnRunErrorKey) = ("confDir", "spark", "stopOnErrorFormat", "stopOnRunError")
  private val formatFile = ".json"
  private val help =
    """
      |This program launches other programs, based on their configuration, western in json format
      |====================================================================================
      |--help                           - parameter for displaying this help
      |--confDir [path]                 - parameter pointing to the configuration directory
      |--spark [path]                   - specifies the path to spark-submit, can be omitted
      |--stopOnErrorFormat [true/false] - pause the program on erroneous input? Default true
      |--stopOnRunError [true/false]    - stop the program if an error occurs as a result of startup
      |""".stripMargin

  def reactToAnError(stopProgram: Boolean = true, error: => Boolean = true, msg: String): Unit = {
    if (error) {
      if (stopProgram) {
        throw new RuntimeException(msg)
      } else {
        println(s"!!! $msg")
      }
    }
  }
}

class ArgsParser(args: Array[String]) {

  val mapOption = nextOption(args.toList)
  val sparkSubmit = mapOption.getOrElse(sparkKey, "spark-submit")
  val stopOnErrorFormat = mapOption.getOrElse(stopOnErrorFormatKey, true).toString.toBoolean
  val stopOnRunError = mapOption.getOrElse(stopOnRunErrorKey, true).toString.toBoolean

  if (mapOption.contains(sparkKey)) {
    reactToAnError(error = !new File(mapOption(sparkKey)).canExecute, msg = "Not found spark-submit")
  }
  reactToAnError(error = !mapOption.contains(confDirKey), msg = "Key confDir not found")
  reactToAnError(error = !new File(mapOption(confDirKey)).isDirectory, msg = "Not found dir with configs")

  val confDir = new File(mapOption(confDirKey)).listFiles(
    (file: File) => file.exists() && file.getName.endsWith(formatFile)
  ).map(_.toPath.toString)

  @tailrec
  private final def nextOption(list: List[String], map: MapOptional = Map()): MapOptional = {
    list match {
      case Nil => map
      case "--spark" :: value :: tail => nextOption(tail, Map(sparkKey -> value) ++ map)
      case "--confDir" :: value :: tail => nextOption(tail, Map(confDirKey -> value) ++ map)
      case "--stopOnErrorFormat" :: value :: tail => nextOption(tail, Map(stopOnErrorFormatKey -> value) ++ map)
      case "--stopOnRunError" :: value :: tail => nextOption(tail, Map(stopOnRunErrorKey -> value) ++ map)
      case "--help" :: _ => println(help); exit(0)
      case _ => throw new RuntimeException("Not valid optional")
    }
  }
}
