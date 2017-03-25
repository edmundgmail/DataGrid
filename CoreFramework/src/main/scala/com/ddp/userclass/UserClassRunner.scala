package com.ddp.userclass

import com.ddp.access.{JobContext, UserClassParameter, UserClassRunner, UserSparkClassRunner}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.xeustechnologies.jcl.{JarClassLoader, JclObjectFactory}

/**
  * Created by cloudera on 9/4/16.
  */


case class RunUserClass (jclFactory : JclObjectFactory, jcl: JarClassLoader ){
  def run (message: UserClassParameter): Any = {
      jclFactory.create(jcl, message.userClassName).asInstanceOf[UserClassRunner].run()
  }

  def runSpark (sqlContext: SparkSession, message: UserClassParameter): Any = {
    val jc = new JobContext(sqlContext)
    jclFactory.create(jcl, message.userClassName).asInstanceOf[UserSparkClassRunner].run(jc)
  }
}
