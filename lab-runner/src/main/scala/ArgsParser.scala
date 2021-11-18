
import org.apache.commons.cli._

//Усложнено, можно сделать трейт с необходимымы методами, а не класс
//Получается больше оберток, чем нужно
class ArgsParser(args: Array[String]) {

// Функции, переменные стоит группировать по типу в коде, а не разбрасывать их по всему классу, читать код ужасно неудобно
  def confDir(): String = confDir

  def spark(): String = spark

   //Достаточно одного метода, котоый что-то делает, зачем отедльно задавать переменную в виде функции, потом ее присваивать,
  //Зачем столько наслоений?
  def reactToAnErrorFormat(fun: => Boolean, msg: String): Unit = this.reactToAnErrorFormatVar(fun, msg)
  def reactToAnErrorRunProgram(fun: => Boolean, msg: String): Unit = this.reactToAnErrorRunProgramVar(fun, msg)

  var reactToAnErrorFormatVar: (=> Boolean, String) => Unit = _

  //Почему везде var, а не val ?
  private[this] var reactToAnErrorRunProgramVar: (=> Boolean, String) => Unit = _ //Не стоит злоупотреблять _
  private[this] var confDir: String = _
  private[this] var stopOnErrorFormat: Boolean = false
  private[this] var stopOnRunError: Boolean = false
  private[this] var spark: String = _

  //Зачем все время передавать print, что нам это дает?
  private[this] def reactToAnError(stopProgram: Boolean, print: String => Unit)(error: => Boolean = true, msg: String): Unit = {
    if (error) {
      if (stopProgram) {
        throw new RuntimeException(msg)
      } else {
        print(" ! ! !" * 5 + s"\n ! ! ! $msg\n" + " ! ! !" * 5)
      }
    }
  }

  /**
   * Methods checks the data passed to the constructor
   * Accepts higher-order functions as input
   * @param isDir path is directory
   * @param isExecuteFile path is execute file in OS
   * @param exitFun program termination function
   * @param print function for outputting data to the outside world
   */
  def validation(isDir: String => Boolean,
                 isExecuteFile: String => Boolean,
                 exitFun: Int => Unit,
                 print: String => Unit): Unit = {


    val help = new Option("h", "help", false, "parameter for displaying this help")
    val confDir = new Option("cdir", "confDir", true, "parameter pointing to the configuration directory")
    val spark = new Option("sp", "spark", true, "specifies the path to spark-submit, can be omitted")
    val stopOnErrorFormat = new Option("sf", "stopOnErrorFormat", false, "set this key if you want to throw an exception in case of data errors")
    val stopOnRunError = new Option("sae", "stopOnRunError", false, "set this key if you want to throw an exception during incorrect execution of the transformation")


    confDir.setRequired(true)
    confDir.setArgName("path")
    spark.setArgName("path to spark-submit")

    val options = new Options()
      .addOption(help)
      .addOption(confDir)
      .addOption(stopOnRunError)
      .addOption(stopOnErrorFormat)
      .addOption(spark)


    val parser = new DefaultParser()

    def helpPrint() = {
      val formatter = new HelpFormatter()
      print("This program launches other programs, based on their configuration, western in json format")
      print("==========================================================================================")
      formatter.printHelp("Lab-runner", options)
      exitFun(0)
    }

    try {
      val cmd = parser.parse(options, args)

      if (cmd.hasOption(help)) {
        helpPrint()
        return
      }

      this.confDir = cmd.getOptionValue(confDir)

      this.spark = if (cmd.hasOption(spark)) {
        if (!isExecuteFile(cmd.getOptionValue(spark))) {
          throw new RuntimeException(s"Not found spark-submit, path = ${cmd.getOptionValue(spark)} not valid")
        }
        cmd.getOptionValue(spark)
      } else {
        "spark-submit"
      }

      this.stopOnRunError = cmd.hasOption(stopOnRunError)

      this.stopOnErrorFormat = cmd.hasOption(stopOnErrorFormat)

      if (!isDir(this.confDir())) {
        throw new RuntimeException(this.confDir + " is not directory")
      }

      reactToAnErrorFormatVar = reactToAnError(this.stopOnErrorFormat, print) _
      reactToAnErrorRunProgramVar = reactToAnError(this.stopOnRunError, print) _
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        helpPrint()
    }
  }


}

