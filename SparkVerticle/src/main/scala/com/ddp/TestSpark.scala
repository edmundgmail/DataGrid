package com.ddp

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by cloudera on 3/20/17.
  */
object TestSpark {

  def main(args: Array[String])  : Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("myapp")

    System.setProperty("hive.metastore.uris", "thrift://quickstart.cloudera:9083")
    System.setProperty("hive.metastore.warehouse.dir", "hdfs:/user/hive/warehouse")
    System.setProperty("hive.metastore.execute.setugi", "true")

    val sparkSession: SparkSession = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate

    sparkSession.sql("select * from syslogevents")
  }
}
