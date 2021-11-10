
import org.apache.commons.cli._

import java.io.File
import scala.sys.exit

class ArgsParser(args: Array[String]) {
  validation()

  def confDir(): String = confDir

  def spark(): String = spark

  val reactToAnErrorFormat: (=> Boolean, String) => Unit = reactToAnError(stopOnErrorFormat) _

  val reactToAnErrorRunProgram: (=> Boolean, String) => Unit = reactToAnError(stopOnRunError) _

  private[this] var confDir: String = _
  private[this] var stopOnErrorFormat: Boolean = false
  private[this] var stopOnRunError: Boolean = false
  private[this] var spark: String = _


  private[this] def reactToAnError(stopProgram: Boolean)(error: => Boolean = true, msg: String): Unit = {
    if (error) {
      if (stopProgram) {
        throw new RuntimeException(msg)
      } else {
        println(" ! ! ! " * 5)
        println(s" ! ! ! $msg")
        println(" ! ! ! " * 5)
      }
    }
  }

  private[this] def validation(): Unit = {

    val help = new Option("h", "help", false, "parameter for displaying this help")
    val confDir = new Option("cdir", "confDir", true, "parameter pointing to the configuration directory")
    val spark = new Option("s", "spark", true, "specifies the path to spark-submit, can be omitted")
    val stopOnErrorFormat = new Option("sr", "stopOnErrorFormat", false, "set this key if you want to throw an exception in case of data errors")
    val stopOnRunError = new Option("sre", "stopOnRunError", false, "set this key if you want to throw an exception during incorrect execution of the transformation")


    confDir.setRequired(true)
    confDir.setArgName("path")
    spark.setArgName("path to spark-submit")

    val options = new Options()
      .addOption(help)
      .addOption(confDir)
      .addOption(spark)
      .addOption(stopOnRunError)
      .addOption(stopOnErrorFormat)


    val parser = new DefaultParser()

    def helpPrint() = {
      val formatter = new HelpFormatter()
      println("This program launches other programs, based on their configuration, western in json format")
      println("==========================================================================================")
      formatter.printHelp("Lab-runner", options)
      exit(0)
    }

    try {
      val cmd = parser.parse(options, args)

      if (cmd.hasOption(help)) {
        helpPrint()
      }

      this.confDir = cmd.getOptionValue(confDir)

      this.spark = if (cmd.hasOption(spark)) {
        if (!new File(cmd.getOptionValue(spark)).canExecute) {
          throw new RuntimeException("Not found spark-submit")
        }
        cmd.getOptionValue(spark)
      } else {
        "spark-submit"
      }

      this.stopOnRunError = cmd.hasOption(stopOnRunError)

      this.stopOnErrorFormat = cmd.hasOption(stopOnErrorFormat)

      if (!new File(this.confDir).isDirectory) {
        throw new RuntimeException(this.confDir + " is not directory")
      }
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        helpPrint()
    }
  }


}

