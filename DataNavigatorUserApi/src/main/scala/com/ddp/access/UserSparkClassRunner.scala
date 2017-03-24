package com.ddp.access

import org.apache.spark.sql.SparkSession
/**
  * Created by cloudera on 3/23/17.
  */

case class JobContext(spark: SparkSession)

trait UserSparkClassRunner {
  def run ( context: JobContext ):Any
}
