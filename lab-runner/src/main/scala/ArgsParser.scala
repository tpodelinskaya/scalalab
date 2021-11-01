import scala.annotation.tailrec
import scala.sys.exit

trait ArgsParser {
  type MapOptional = Map[String, String]

  def reactToAnError(stopProgram: Boolean = true)(error: => Boolean, msg: String): Unit = {
    if (error) {
      if (stopProgram) {
        throw new RuntimeException(msg)
      } else {
        println(s"!!! $msg")
      }
    }
  }

  val (confDirKey, sparkKey, stopOnErrorFormatKey, stopOnRunErrorKey) = ("confDir", "spark", "stopOnErrorFormat", "stopOnRunError")


  val help =
    """
      |This program launches other programs, based on their configuration, western in json format
      |====================================================================================
      |--help                           - parameter for displaying this help
      |--confDir [path]                 - parameter pointing to the configuration directory
      |--spark [path]                   - specifies the path to spark-submit, can be omitted
      |--stopOnErrorFormat [true/false] - pause the program on erroneous input? Default true
      |--stopOnRunError [true/false]    - stop the program if an error occurs as a result of startup
      |""".stripMargin

  @tailrec
  final def nextOption(list: List[String], map: MapOptional = Map()): MapOptional = {
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
