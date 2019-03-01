package mqz.utils

import java.util
import java.util.Properties

import mqz.entity.Order
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random


object KafkaGenData {

  val TOPIC = "source"
  val ZOOKEEPER_CONNECT = "hadoop003:2181,hadoop004:2181"
  val GROUP_ID = "group1"
  val METADATA_BROKER_LIST = "hadoop003:9092,hadoop004:9092"

  def main(args: Array[String]): Unit = {
    producerGenData(TOPIC)
    //    showConsumerData(TOPIC)
  }

  def producerGenOrder(topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", METADATA_BROKER_LIST)
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    (1 to 100).foreach(i => {
      print(".")
      val order = Order(i, "pen", Random.nextInt(100))
      val msg = new ProducerRecord(topic,"key",order.toString)
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
