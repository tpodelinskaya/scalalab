import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class MainSpec extends AnyFlatSpec with should.Matchers {
  val inpath =  "C:\\Users\\valera\\IdeaProjects\\scalalab\\lab-tr02\\src\\main\\scala\\Simple.html"
  val outpath = "out_path"
  val argsValid = Array(s"inpath=$inpath", s"outpath=$outpath")

  "Args" should "formatted" in {
    val argsMap = Main.formatArgs(argsValid)

    println(argsMap)

    Main.getArg(argsMap, "inpath")
    Main.getArg(argsMap, "outpath")
  }
}