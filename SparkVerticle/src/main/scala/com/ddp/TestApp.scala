package com.ddp

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by cloudera on 3/3/17.
  */
object TestApp {
  def main(args: Array[String])  : Unit = {
    /*
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val conf = new SparkConf().setMaster("yarn").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))


    val topics = Array("topicA")
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)

    stream.map(_._2).print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait
    */
  }

}
