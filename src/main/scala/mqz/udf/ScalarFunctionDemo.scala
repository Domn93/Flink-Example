package mqz.udf

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row



object TableFunctionDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val sourceData = List(
      (1, "Pink Floyd"),
      (2, "Dead Can Dance"),
      (3, "The Doors"),
      (4, "Dou Wei"),
      (5, "Cui Jian")
    )

    val albumsData = List(
      ("Pink Floyd", "The Dark Side of the Moon"),
      ("Pink Floyd", "Wish You Were Here"),
      ("Dead Can Dance", "Aion"),
      ("Nirvana", "Nevermind")
    )

    tableEnv.registerFunction("UpperUdf", new UpperUdf)
    tableEnv.registerFunction("LowerUdf", new LowerUdf)

    val t1 = env.fromCollection(sourceData)
      .toTable(tableEnv)
      .as('id, 'band)

    tableEnv.registerTable("T1",t1)

    val sqlQuery =
      """
        | SELECT
        |   COALESCE(UpperUdf(band), LowerUdf(band)),Configuration
        |   UpperUdf(band),
        |   LowerUdf(band)
        | FROM T1
        | WHERE id = 1 AND LowerUdf(band) = 'pink floyd'
      """.stripMargin

    val result = tableEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.print()
    env.execute()
  }

}

class UpperUdf extends ScalarFunction {
  def eval(value: String): String = {
    value.toUpperCase()
  }
}

class LowerUdf extends ScalarFunction {
  def eval(value: String): String = {
    value.toLowerCase()
  }
}

class NonDeterministicUdf extends ScalarFunction {

  override def isDeterministic: Boolean = false

  def eval(value: String): String = {
    value + " - " + (new scala.util.Random).nextInt(10000)
  }
}

