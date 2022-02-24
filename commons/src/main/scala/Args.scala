import scala.util.matching.Regex

object Args {
  type Args = Map[String, String]

  def getArg(args: Args, nameArg: String): String = {
    args.getOrElse(nameArg, throw new RuntimeException(s"not set variable '$nameArg'"))
  }

  def formatArgs(argsArr:  Array[String]): Map[String, String] = {
    val keyValPattern: Regex = "^([\\w]+)=(.+)$".r
    argsArr.map(arg => {
      val keyVal = keyValPattern.findPrefixMatchOf(arg).getOrElse(throw new RuntimeException("not valid arg"))
      (keyVal.group(1), keyVal.group(2))
    }).toMap
  }
}
