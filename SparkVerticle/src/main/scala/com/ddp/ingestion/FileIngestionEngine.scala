package com.ddp.ingestion

import com.ddp.access.{CsvIngestionParameter, xmlIngestionParameter}
import com.ddp.utils.Utils
import com.google.gson.Gson
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.json4s.jackson.Json

/**
  * Created by cloudera on 3/30/17.
  */
case class MetaObject(sname : String)

case class FileIngestionEngine (sqlContext : SQLContext){

  def ingestCsv(fileIngestionParameter: CsvIngestionParameter): Any ={

    val tempView = fileIngestionParameter.tableName + Utils.getRandom
    val option = if(fileIngestionParameter.hasHeader) "true" else "false"
    if(fileIngestionParameter.schema !=null && !fileIngestionParameter.schema.isEmpty){
      val s = new Gson().fromJson(fileIngestionParameter.schema, classOf[Array[MetaObject]])
      val schema = StructType(s.map(f=>new StructField(f.sname, DataTypes.StringType, true)))
      val df = sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", option) // Use first line of all files as header
        .schema(schema)
        .load(fileIngestionParameter.filePath)
        .createOrReplaceTempView(tempView)

      sqlContext.sql("CREATE TABLE " + fileIngestionParameter.tableName + " SELECT * FROM " + tempView )

    }else{
      val sql = "CREATE TABLE " + fileIngestionParameter.tableName + " USING com.databricks.spark.csv OPTIONS (path \"" + fileIngestionParameter.filePath + "\", header \"true\", inferSchema \"true\")"
      sqlContext.sql(sql)
    }

    if(fileIngestionParameter.returnSampleSize>0){
      val df = sqlContext.sql("SELECT * FROM " + fileIngestionParameter.tableName + " limit " + fileIngestionParameter.returnSampleSize)
      df.show(fileIngestionParameter.returnSampleSize)
      return df.take(fileIngestionParameter.returnSampleSize)

    }
  }

   def ingestXml(fileIngestionParameter: xmlIngestionParameter): Any ={
     val tempView = fileIngestionParameter.tableName + Utils.getRandom
     val option = if(fileIngestionParameter.hasHeader) "true" else "false"
     if(fileIngestionParameter.schema !=null && !fileIngestionParameter.schema.isEmpty){
       val s = new Gson().fromJson(fileIngestionParameter.schema, classOf[Array[MetaObject]])
       val schema = StructType(s.map(f=>new StructField(f.sname, DataTypes.StringType, true)))
       val df = sqlContext.read
         .format("com.databricks.spark.xml")
         .option("header", option) // Use first line of all files as header
         .option("rootTag", fileIngestionParameter.rootTag)
         .option("rowTag", fileIngestionParameter.rowTag)
         .schema(schema)
         .load(fileIngestionParameter.filePath)
         .createOrReplaceTempView(tempView)

       sqlContext.sql("CREATE TABLE " + fileIngestionParameter.tableName + " SELECT * FROM " + tempView )

     }else{
     val sql = "CREATE TABLE " + fileIngestionParameter.tableName + " USING com.databricks.spark.xml OPTIONS (path \"" + fileIngestionParameter.filePath +
       "\", rootTag \"" + fileIngestionParameter.rootTag +
       "\", rowTag \"" + fileIngestionParameter.rowTag +
       "\")"
      sqlContext.sql(sql)
    }

     if(fileIngestionParameter.returnSampleSize>0)
       return sqlContext.sql("SELECT * FROM " + fileIngestionParameter.tableName + " limit " + fileIngestionParameter.returnSampleSize)
   }
}