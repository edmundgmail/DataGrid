package com.ddp.ingestion

import com.ddp.access.{csvIngestionParameter, xmlIngestionParameter}
import org.apache.spark.sql.SQLContext

/**
  * Created by cloudera on 3/30/17.
  */
case class FileIngestionEngine (sqlContext : SQLContext){

  def ingestCsv(fileIngestionParameter: csvIngestionParameter): Any ={
    val sql = "CREATE TABLE " + fileIngestionParameter.tableName + " USING com.databricks.spark.csv OPTIONS (path \"" + fileIngestionParameter.filePath + "\", header \"true\", inferSchema \"true\")"
    sqlContext.sql(sql)
  }

   def ingestXml(fileIngestionParameter: xmlIngestionParameter): Any ={
     val sql = "CREATE TABLE " + fileIngestionParameter.tableName + " USING com.databricks.spark.xml OPTIONS (path \"" + fileIngestionParameter.filePath +
       /*"\", rootTag \"" + fileIngestionParameter.rootTag +*/
       "\", rowTag \"" + fileIngestionParameter.rowTag +
       "\")"
      sqlContext.sql(sql)
   }
}