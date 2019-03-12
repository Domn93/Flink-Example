package mqz.connector

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.types.Row
import scala.collection.JavaConversions._


/**
  * @author maqingze
  * @version v1.0
  * @date 2019/3/7 11:24
  */
object KafkaConnector {
  private val SOURCE_TOPIC = "source"
  private val SINK_TOPIC = "sink"
  private val ZOOKEEPER_CONNECT = "hadoop003:2181,hadoop004:2181"
  private val GROUP_ID = "group1"
  private val METADATA_BROKER_LIST = "hadoop003:9092,hadoop004:9092"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.connect(
      new Kafka()
        .version("0.10")
        .topic(SOURCE_TOPIC)
        .startFromEarliest
        .property("zookeeper.connect", ZOOKEEPER_CONNECT)
        .property("bootstrap.servers", METADATA_BROKER_LIST))
      .withFormat(
        //            new Csv()
        //                .field("id",Types.STRING)
        //                .field("product", Types.STRING)
        //                .field("amount", Types.STRING)
        //                .fieldDelimiter(",")              // optional: string delimiter "," by default
        //                .lineDelimiter("\n")              // optional: string delimiter "\n" by default
        //                .quoteCharacter('"')              // optional: single character for string values, empty by default
        //                .ignoreFirstLine()                // optional: ignore the first line, by default it is not skipped
        //                .ignoreParseErrors()
        new Json()
          .schema(org.apache.flink.table.api.Types.ROW(Array[String]("id", "product", "amount")
            , Array[TypeInformation[_]](org.apache.flink.table.api.Types.LONG, org.apache.flink.table.api.Types.STRING
              , org.apache.flink.table.api.Types.INT)))
          .failOnMissingField(true))
      .withSchema(
        new Schema()
          .field("id", Types.LONG)
          .field("product", Types.STRING)
          .field("amount", Types.INT)
      )
      .inAppendMode()
      .registerTableSource("sourceTable")
    val table: Table = tEnv.sqlQuery("select * from sourceTable limit 1")

    table.toAppendStream[Row].print()


//    result.writeToSink(
//      new CsvTableSink(
//        "D:\\Aupload\\flink\\test_3col_op8.txt", // output path
//        fieldDelim = "|", // optional: delimit files by '|'
//        numFiles = 1, // optional: write to a single file
//        writeMode = WriteMode.OVERWRITE)
//    )


    env.execute(" tesst kafka connector demo")
  }


}
