package mqz

import java.util.Properties

import mqz.entity.Order
import mqz.udf.{LowerUdf, UpperUdf}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row


object Source2Table2Sink {
  def main(args: Array[String]): Unit = {
    val sourceTopic = "source"
    val sinkTopic = "sink"
    val sourceProp = new Properties()
    sourceProp.setProperty("bootstrap.servers", "hadoop003:9092")
    sourceProp.setProperty("group.id", "con1")
    sourceProp.put("auto.offset.reset", "latest")


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)



    //checkpoint配置
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    // kafka Source
    val comsumer = new FlinkKafkaConsumer010[String](sourceTopic, new SimpleStringSchema(), sourceProp)
    val kafkaSource = env.addSource(comsumer)
    var orderA = kafkaSource.map(line => {
      println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% "+line)
      val s:Array[String] = line.split("\\|")
      println("*******************"+ s(0)+s(1)+s(2))
      Order(s(0).toLong,s(1),s(2).toInt)
    }).toTable(tEnv)
    // static data source
    val orderB =  env.fromCollection(Seq(
      Order(1L, "pear", 2),
      Order(2L, "pen", 3),
      Order(31L, "beer", 2),
      Order(22L, "rubber", 6),
      Order(2L, "pen", 3),
      Order(2L, "rubber", 3),
      Order(4L, "beer", 1))).toTable(tEnv)


    // UDF注册
    tEnv.registerFunction("UpperUdf", new UpperUdf)
    tEnv.registerFunction("LowerUdf", new LowerUdf)

    // 表注册
    tEnv.registerTable("StaticSource", orderB)
    tEnv.registerTable("KafkaSource", orderA)

    // SQL
    val sqlQuery =
      """
        | SELECT a.id,UpperUdf(a.product),a.amount
        | FROM KafkaSource AS a,StaticSource AS b
        | WHERE a.amount > 2 AND a.id = b.id
      """.stripMargin
//    val sqlQuery =
//      """
//        | SELECT user,UpperUdf(product),amount
//        | FROM StaticSource
//      """.stripMargin
    println(s"sql is = $sqlQuery")
    val resultTable = tEnv.sqlQuery(sqlQuery)
    val resultDS: DataStream[Row] = resultTable.toAppendStream[Row]
    val res = resultDS.map(row=>{
      row.toString
    })


    // Sink
    val myProducer = new FlinkKafkaProducer010[String](sinkTopic, new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()), sourceProp)

    val sink = res.addSink(myProducer)


    // Execute
    env.execute("kafka -> table -> sql -> kafka")

  }



}
