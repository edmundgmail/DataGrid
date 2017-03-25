package com.ddp.cpybook

import com.ddp.access.UserClassRunner
import com.ddp.access.CopybookIngestionParameter
import com.ddp.utils.Utils
import org.apache.hadoop
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by cloudera on 9/3/16.
  */




  case class CopybookIngestion (sqlContext : SQLContext) {

    def run (param: CopybookIngestionParameter) : Any = {
      val conf = Utils.getHdfsConf

      conf.set(Constants.CopybookName, param.cpyBookName)
      conf.set(Constants.CopybookHdfsPath, param.cpyBookHdfsPath  )
      conf.set(Constants.CopybookFileStructure, param.fileStructure)
      conf.set(Constants.CopybookBinaryformat, param.binaryFormat)
      conf.set(Constants.CopybookSplitOpiton, param.splitOptoin)
      conf.set(Constants.DataFileHdfsPath, param.dataFileHdfsPath)
      conf.set(Constants.CopybookFont, param.cpybookFont)

      conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem")


      val tempTable = param.cpyBookName + "_temp_1"

      //Future {
        try {
          val trips = sqlContext.cbFile(conf)
          //trips.createOrReplaceTempView(tempTable)
          trips.cache()
          }
          catch {
          case e: Throwable => e.printStackTrace()
        }
      //}


      //sqlContext.sql("select * from " + tempTable)

       //trips.createOrReplaceTempView(tempTable)
      //hc.sql("select * from " + tempTable)
      //hc.sql("create table " + param.cpyBookName + " as select * from " + tempTable )
      //sqlContext.sql("select * from " + tempTable)
      //sqlContext.sql("create table " + param.cpyBookName + " as select * from " + tempTable )
      //System.out.println("done create table")

    }
}
