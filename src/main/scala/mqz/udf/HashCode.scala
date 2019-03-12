package mqz.udf

import mqz.entity.Order
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment, TableSchema}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row


class HashCode extends ScalarFunction {
  def eval(s: String): Int = {
    s.hashCode() * 10
  }
}

object HashCode {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val orderA: DataStream[Order] = env.fromCollection(Seq(
      Order(2L, "pen", 3),
      Order(2L, "rubber", 3),
      Order(4L, "beer", 1)))

    val table: Table = orderA.toTable(tEnv)
    tEnv.registerTable("orderA",table)
    tEnv.registerFunction("hashCode", new HashCode)
    tEnv.registerFunction("UpperUdf", new UpperUdf)
    val udf = tEnv.sqlQuery("SELECT user,hashCode(product) FROM orderA")
    udf.toAppendStream[Row].print()

    env.execute(" test udf ")


  }


}

