import com.google.gson.{JsonObject, JsonParser}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class JsonConvertSpec extends AnyFlatSpec with should.Matchers {
  val createCommand: JsonObject => String = JsonConvert.createCommand("spark-submit") _

  val jsonSimpleConfig: JsonObject = JsonParser.parseString(
    """
      |{
      |  "jar-file-path" : "lab_tr01.jar",
      |  "class" : "Main",
      |  "app-name" : "LoadTr01App",
      |  "spark-params" : {
      |    "executor-cores" : "1",
      |    "executor-memory" : "1G",
      |    "driver-memory" : "1G"
      |  },
      |  "tr-params" : {
      |    "jbdc-url" : "jdbc:postgresql://127.0.0.1:5432/db",
      |    "jdbc-user" : "admin"
      |  }
      |}
      |""".stripMargin).getAsJsonObject

  val jsonArrayParameterConfig: JsonObject = JsonParser.parseString(
    """
      |{
      |  "jar-file-path" : "lab_tr03.jar",
      |  "class" : "Main",
      |  "app-name" : "ArrayTest",
      |  "spark-params" : {
      |    "executor-cores" : "1",
      |    "arrayParameter" : ["element1", "element2", "element3"]
      |  },
      |  "tr-params" : { }
      |}
      |""".stripMargin).getAsJsonObject

  val jsonNotDataSetClass: JsonObject = JsonParser.parseString(
    """
      |{
      |  "jar-file-path" : "lab_tr03.jar",
      |  "app-name" : "LoadTr01App",
      |  "tr-params" : { }
      |}
      |""".stripMargin).getAsJsonObject

  val jsonNotValidData: JsonObject = JsonParser.parseString(
    """{
      |  "jar-file-path": "lab_tr04.jar",
      |  "class": "Main",
      |  "app-name": "LoadTr01App",
      |  "spark-params": {
      |    "executor-cores": "1",
      |    "arrayParameter": [
      |      "element1",
      |      "element2",
      |      "element3"
      |    ],
      |    "arrArray": [
      |      {
      |        "elem": "elem",
      |        "bin": "bin"
      |      }
      |    ]
      |  },
      |  "tr-params": {}
      |}""".stripMargin).getAsJsonObject


  "Json convert" should "create command" in {
    val command = createCommand(jsonSimpleConfig)
    println(s"Command run (SimpleConfig) = $command")
    command should be("spark-submit  --name \"LoadTr01App\"  --executor-cores 1 " +
      "--executor-memory 1G --driver-memory 1G  --class Main" +
      "  lab_tr01.jar jbdc-url=jdbc:postgresql://127.0.0.1:5432/db jdbc-user=admin")
  }

  it should "convert array to parameter" in {
    val command = createCommand(jsonArrayParameterConfig)
    println(s"Command run (parameter config) = $command")
    command should be("spark-submit  --name \"ArrayTest\"  --executor-cores 1 --arrayParameter element1 element2 element3  --class Main  lab_tr03.jar ")
  }

  it should "return error (not set data)" in {
    intercept[ExceptionParseJson] {
      createCommand(jsonNotDataSetClass)
    }
  }

  it should "return error (not valid type)" in {
    intercept[ExceptionParseJson] {
      createCommand(jsonNotValidData)
    }
  }
}
