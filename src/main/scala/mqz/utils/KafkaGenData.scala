package mqz.utils

import java.util
import java.util.Properties

import com.alibaba.fastjson.JSON
import mqz.JavaOrder
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random


object KafkaGenData {

  val TOPIC = "source"
//  val ZOOKEEPER_CONNECT = "hadoop003:2181,hadoop004:2181"
  val GROUP_ID = "group1"
  //  val METADATA_BROKER_LIST = "hadoop003:9092,hadoop004:9092"
  val METADATA_BROKER_LIST = "172.16.13.116:9092,172.16.13.117:9092,172.16.13.118:9092,"
  val ZOOKEEPER_CONNECT = "172.16.13.116:2181/kafka011"

  val ORDER_KIND = List(
    "pear"
    , "pen"
    , "rubber"
    , "beer"
  )


  def main(args: Array[String]): Unit = {
    producerGenOrderJSON(TOPIC)
    //    showConsumerData(TOPIC)
  }

  def producerGenOrderJSON(topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", METADATA_BROKER_LIST)
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    (1 to 100).foreach(i => {
      print(".")
//      val javaOrder = new JavaOrder(i.toLong, "pen", Random.nextInt(5));
//      val order = Order(i, "pen", Random.nextInt(5))

//      val json = s"{\"id\":$i,\"product\":\"pen\",\"amount\":${Random.nextInt(5)}}"

//      val json = "{\"id\":2,\"product\":\"peer\",\"amount\":5}"
      val json = "{\"sno\":\"2\",\"name\":\"zhangsan\",\"age\":\"5\"}"
      val msg = new ProducerRecord(topic, "key",json)
      producer.send(msg)
      Thread.sleep(1000)
    })
    producer.close()

  }

  def producerGenOrderText(topic: String): Unit = {
    val spliter = "|"
    val props = new Properties()
    props.put("bootstrap.servers", METADATA_BROKER_LIST)
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    (1 to 100).foreach(i => {
      print(".")
      val orderStr = i + spliter + ORDER_KIND(Random.nextInt(ORDER_KIND.length)) + spliter + Random.nextInt(5)
      //      val order = Order(i, "pen", Random.nextInt(5))
      val msg = new ProducerRecord(topic, "key", orderStr)
      producer.send(msg)
      Thread.sleep(1000)
    })
    producer.close()
  }

  def producerGenData(topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", METADATA_BROKER_LIST)
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    (1 to 100).foreach(i => {
      print(".")
      val msg = new ProducerRecord(topic, "key", "msg" + i)
      producer.send(msg)
      Thread.sleep(1000)
    })
    producer.close()
  }

  def showConsumerData(topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", METADATA_BROKER_LIST)
    props.put("group.id", "test")
    props.put("enable.auto.commit", "false")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "earliest")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(topic))
    while (true) {
      val records = consumer.poll(100)
      val it = records.iterator();
      while (it.hasNext) {
        val record: ConsumerRecord[String, String] = it.next()
        val fs = printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value())
        println(fs)
      }
    }
  }

}
