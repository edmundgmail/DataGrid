package com.ddp.userclass

import com.ddp.access.QueryParameter
import com.ddp.utils.Utils
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by cloudera on 3/20/17.
  */
case class Query(sqlContext:SparkSession, param : QueryParameter){

  def query : Any = {
    val path = Utils.getTempPath()
      try {
        sqlContext.sql(param.sql).write.json(path)

      }
      catch
        {
          case _ :Throwable =>
        }

      return path
    }

}
