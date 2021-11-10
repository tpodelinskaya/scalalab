import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.sys.exit

class ArgsParserSpec extends AnyFlatSpec with should.Matchers {

  implicit def arrayParser(array: Array[String]): ArgsParser = new ArgsParser(array)

  val argsStopError: ArgsParser = Array("-cdir", "/home/confDir", "-sr", "-src")
  val argsNotStopError: ArgsParser = Array("-cdir", "/home/confDir2")
  val argsStopRunError: ArgsParser = Array("-cdir", "\\home\\co_nf=Dir3", "--stopOnRunError")

  val argsHelp: ArgsParser = Array("--help")

  val argsErrorNoneDir: ArgsParser = Array("--stopOnErrorFormat")
  val argsErrorNotValid: ArgsParser = Array("-cdir", "-sr")

  "Args parser" should "accept parameters stop on error" in {
    var msgCheck: String = ""
    def printMock(msg: String) = {
      msgCheck = msg
      println(msg)
    }

    argsStopError.validation((_) => true, (_) => true, exit(_), printMock(_))

/*    val msgError = "1 == 2"
    intercept[RuntimeException] {
      argsStopError.reactToAnErrorFormatVar(true, msgError)
//      argsStopError.reactToAnErrorFormat(1 == 2, msgError)
    }
    msgCheck.contains(msgError) should be (true)*/
  }
}
