package com.ddp

import org.apache.spark.deploy.SparkSubmit

import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
  * Created by cloudera on 3/3/17.
  */
object A{
  private def method1():Unit = {
      System.out.println("hello,wworld")
  }
}

class PrivateMethodCaller(x: AnyRef, methodName: String) {
  def apply(_args: Any*): Any = {
    val args = _args.map(_.asInstanceOf[AnyRef])
    def _parents: Stream[Class[_]] = Stream(x.getClass) #::: _parents.map(_.getSuperclass)
    val parents = _parents.takeWhile(_ != null).toList
    val methods = parents.flatMap(_.getDeclaredMethods)
    val method = methods.find(_.getName == methodName).getOrElse(throw new IllegalArgumentException("Method " + methodName + " not found"))
    method.setAccessible(true)
    method.invoke(x, args : _*)
  }
}

class PrivateMethodExposer(x: AnyRef) {
  def apply(method: scala.Symbol): PrivateMethodCaller = new PrivateMethodCaller(x, method.name)
}


object TestApp {


/*
  def main(args: Array[String]) {

    val config = new Configuration()
    fillProperties(config, getPropXmlAsMap("config/core-site.xml"))
    fillProperties(config, getPropXmlAsMap("config/yarn-site.xml"))

    System.setProperty("SPARK_YARN_MODE", "true")

    val sparkConf = new SparkConf()
    val cArgs = new ClientArguments(args, sparkConf)

    new Client(cArgs, config, sparkConf).run()

  }
*/




  def main(args: Array[String])  : Unit = {

    //val x = new PrivateMethodCaller(A, "method1")
    //x.apply()

    val y = new PrivateMethodExposer(SparkSubmit)
    //public void org.apache.spark.deploy.SparkSubmit$.org$apache$spark$deploy$SparkSubmit$$runMain(scala.collection.Seq,scala.collection.Seq,scala.collection.mutable.Map,java.lang.String,boolean)
    val childArgs = new ArrayBuffer[String]()
    val childClasspath = new ArrayBuffer[String]()
    val sysProps = new HashMap[String, String]()
    sysProps.put("spark.master", "yarn")
    sysProps.put("spark.submit.deployMode", "client")
    sysProps.put("spark.app.name", "verticle")
    sysProps.put("spark.driver.extraClassPath", "/home/cloudera/workspace/DataGrid/SparkVerticle/target/")

    var childMainClass = "com.ddp.SparkVerticle"
    childArgs+="-conf /home/cloudera/workspace/DataGrid/SparkVerticle/src/main/resources/dev/app-conf.json"
    childClasspath+="/home/cloudera/workspace/DataGrid/SparkVerticle/target/SparkVerticle-0.1-fat.jar"
    childClasspath+="/usr/local/spark2/jars/*"
    System.out.println("ispython=" + y.apply('org$apache$spark$deploy$SparkSubmit$$runMain).apply( childArgs, childClasspath, sysProps,childMainClass, true))


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
