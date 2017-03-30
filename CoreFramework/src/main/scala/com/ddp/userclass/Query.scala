package com.ddp.userclass

import com.ddp.access.QueryParameter
import com.ddp.utils.Utils
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by cloudera on 3/20/17.
  */
case class Query(sqlContext:SparkSession){

  def query(sql:String) : Any = {
    val path = Utils.getTempPath
      try {
        val ret = sqlContext.sql(sql)
        ret.show(100)
        ret.write.json(path)
      }
      catch
        {
          case e :Throwable => {
            e.printStackTrace
            System.err.println(e.getMessage)
          }
        }

      return path
    }

}
