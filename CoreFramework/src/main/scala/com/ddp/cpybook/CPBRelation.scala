package com.ddp.cpybook

import net.sf.JRecord.External.Def.ExternalField
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan, TableScan}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
 * Extends `PrunedScan` to map the RDD to specified columns.
 * @param location the location of the dbf file
 * @param sqlContext the SQL context
 *
 * TODO - extend PrunedFilterScan to apply 'smart' filter !
 */
case class CPBRelation(conf: Configuration)(@transient val sqlContext: SQLContext) extends BaseRelation with TableScan with PrunedScan{

  private[ddp] def toStructField(field: ExternalField): StructField = {
    StructField(field.getName, toDataType(field))
  }

  private[ddp] def toDataType(field: ExternalField): DataType = {
    field.getType() match {
      case _ => StringType
    }
  }

  def fields : Array[ExternalField] = {
     CopybookHelper.getExternalRecord(conf).getRecordFields
  }


  private def indexOf (name: String): Int =fields.indexWhere(x=>x.getName.equals(name))

  /**
   * Determine the RDD Schema based on the DBF header info.
   * @return StructType instance
   */
  override def schema : StructType = {

      StructType(fields.map(toStructField))
  }

  /**
   * This is depending on Hadoop's implementation of InputFormat in the Shapefile project
   *
   * TODO - Optimize in Shapefile lib to return List(values) rather than Map(field name -> value)
   */


  private [ddp] val baseRDD = {
    sqlContext.sparkContext.newAPIHadoopFile(conf.get(Constants.DataFileHdfsPath), classOf[CopybookInputFormat], classOf[LongWritable], classOf[Map[String, String]], conf)
  }

  override def buildScan(): RDD[Row] = buildScan(Array.empty)

  override def buildScan(requiredColumns: Array[String]) = baseRDD.map(record => {
    Row.fromSeq(requiredColumns.map { col => record._2.get(col).get.toString})
  })
}