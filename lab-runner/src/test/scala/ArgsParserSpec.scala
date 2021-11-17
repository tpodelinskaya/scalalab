import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.language.implicitConversions
import scala.sys.exit

class ArgsParserSpec extends AnyFlatSpec with should.Matchers {
  def isDirMock(argsParser: ArgsParser)(path: String): Boolean = {
    argsParser.confDir() == path
  }

  def isExecuteSparkMock(path: String): Boolean = {
    path.contains("spark-submit")
  }

  implicit def arrayParser(array: Array[String]): ArgsParser = new ArgsParser() {
    override def args(): Array[String] = {
      array
    }
  }

  val argsStopError: ArgsParser = Array("-cdir", "/home/confDir", "-sf", "-sae")
  val argsNotStopError: ArgsParser = Array("-cdir", "/home/confDir2")
  val argsStopRunError: ArgsParser = Array("--confDir", "\\home\\co_nf=Dir3", "--stopOnRunError")

  val argsHelp: ArgsParser = Array("--help")

  val argsErrorNoneDir: ArgsParser = Array("--stopOnErrorFormat")
  val argsErrorNotValid: ArgsParser = Array("-cdir", "-sr")

  "Args parser" should "accept parameters stop on error" in {
    argsStopError.validation(isDirMock(argsStopError), isExecuteSparkMock, exit(_), _ => {})
    argsStopRunError.validation(isDirMock(argsStopRunError), isExecuteSparkMock, exit(_), _ => {})

    argsStopError.confDir() should be("/home/confDir")

    argsStopError.spark() should be("spark-submit")

    val msgError = "1 != 2"
    intercept[java.lang.RuntimeException] {
      argsStopError.reactToAnErrorFormat(1 != 2, msgError)
    }
    intercept[java.lang.RuntimeException] {
      argsStopError.reactToAnErrorRunProgram(1 != 2, msgError)
    }

    intercept[java.lang.RuntimeException] {
      argsStopRunError.reactToAnErrorRunProgram(1 != 2, msgError)
    }
  }

  it should "print error" in {
    var out: String = ""

    def printMock(msg: String) = {
      out = msg
      println(msg)
    }

    argsNotStopError.validation(isDirMock(argsNotStopError), isExecuteSparkMock, exit(_), printMock)

    val msgError = "1 != 2 (run)"
    argsNotStopError.reactToAnErrorRunProgram(1 != 2, msgError)
    out.contains(msgError) should be(true)

    val msgErrorFormat = "1 != 2f"
    argsNotStopError.reactToAnErrorFormat(1 != "2f", msgErrorFormat)
    out.contains(msgErrorFormat) should be(true)
  }

  it should "help print" in {
    argsHelp.validation(_ => false, _ => false, i => {
      i should be(0)
      Unit
    }, println(_))
  }

  it should "throw exception" in {
    argsErrorNoneDir.validation(isDirMock(argsErrorNoneDir), isExecuteSparkMock, i => {
      i should be(0)
      Unit
    }, println(_))

    argsErrorNotValid.validation(isDirMock(argsErrorNotValid), isExecuteSparkMock, i => {
      i should be(0);
      Unit
    }, println(_))

  }
}
