import Args.{formatArgs, getArg}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class MainSpec extends AnyFlatSpec with should.Matchers {
  val inpath =  "/home/./test=dir\\win\\"
  val mask = "[^t=]"
  val outPath = "out_path\\path\\path"
  val argsValid = Array(s"inpath=$inpath", s"mask=$mask", s"outpath=$outPath")

  "Args" should "formatted" in {
    val argsMap = formatArgs(argsValid)

    println(argsMap)

    getArg(argsMap, "inpath")
    getArg(argsMap, "mask")
    getArg(argsMap, "outpath")
  }
}
