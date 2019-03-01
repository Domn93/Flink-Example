package mqz

import java.util.Properties

import mqz.entity.Order
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper
import org.apache.flink.table.api.TableEnvironment

object Source2Table2Sink {
  def main(args: Array[String]): Unit = {
    val sourceTopic = "source"
    val sinkTopic = "sink"
    val sourceProp = new Properties()
    sourceProp.setProperty("bootstrap.servers", "hadoop003:9092")
    sourceProp.setProperty("group.id", "con1")
    sourceProp.put("auto.offset.reset", "earliest")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    import org.apache.flink.api.scala._


    //checkpoint配置
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    // kafka Source
    val comsumer = new FlinkKafkaConsumer010[String](sourceTopic, new SimpleStringSchema(), sourceProp)
    val text = env.addSource(comsumer)
    text.map(line => {
      val s:Array[String] = line.split("|")
      println("==="+s(1))
    })
    // static data source
    val orderB: DataStream[Order] = env.fromCollection(Seq(
      Order(2L, "pen", 3),
      Order(2L, "rubber", 3),
      Order(4L, "beer", 1)))
    tEnv.registerDataStream("StaticSource", orderB)
    // SQL
    text.print()


    // Sink
    val myProducer = new FlinkKafkaProducer010[String](sinkTopic, new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()), sourceProp)
    text.addSink(myProducer)

    // Execute
    env.execute("kafka -> table -> sql -> kafka")
  }



}
