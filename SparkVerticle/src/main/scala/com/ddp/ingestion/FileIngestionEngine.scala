package com.ddp.ingestion

import com.ddp.access.FileIngestionParameter
import org.apache.spark.sql.SQLContext

/**
  * Created by cloudera on 3/30/17.
  */
case class FileIngestionEngine (sqlContext : SQLContext){

  def run(param: FileIngestionParameter): Any ={
   param.format match{
     case "csv" => csvIngestion(param)
     case "xml"=>xmlIngestion(param)
   }

  }

  private def csvIngestion(fileIngestionParameter: FileIngestionParameter): Any ={
    val sql = "CREATE TABLE " + fileIngestionParameter.tableName + " USING com.databricks.spark.csv OPTIONS (path \"" + fileIngestionParameter.filePath + "\", header \"true\", inferSchema \"true\")"
    sqlContext.sql(sql)
  }

   private def xmlIngestion(fileIngestionParameter: FileIngestionParameter): Any ={
     val sql = "CREATE TABLE " + fileIngestionParameter.tableName + " USING com.databricks.spark.xml OPTIONS (path \"" + fileIngestionParameter.filePath + "\", header \"true\", inferSchema \"true\")"
      sqlContext.sql(sql)
   }
}