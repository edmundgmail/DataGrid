package com.ddp.access

import java.util.function.Consumer

trait UserParameter {
	val className:String
}


case class HiveHierarchyParameter (override val className: String, val level: String, val sname: String) extends UserParameter

case class BaseConsumer(val baseRequest: BaseRequest, val responseHandler : Consumer[String])
case class BaseRequest(sessionKey : Long,  parameter: UserParameter, needPadding: Boolean = false)

case class NewDataSourceParameter(override val className:String, level: String, name: String, desc: String, sourceId:Long) extends UserParameter
case class UserScriptParameter(override val className:String, action: String, level : String, name : String, id: Integer, parentId: Integer, content:String) extends UserParameter

case class UserClassParameter(override val className:String, userClassName:String, useSpark : Boolean = false) extends UserParameter

/*
trait JobParameter extends  UserParameter{
	val name:String
	val group: String
}
case class GridJobKey(override val className:String, override val name:String ,override val group:String) extends JobParameter
case class GridJobDescriptor(override val className:String, override val name:String ,override val group:String, val triggers: List[TriggerDescriptor], val data: Map[String, Any]) extends JobParameter

case class TriggerDescriptor(override val className:String, name:String ,group:String) extends JobParameter
*/

trait IngestionParameter extends UserParameter{
  val filePath:String
  val tableName : String
	val templateTableName : String
  var schema : String
	val hasHeader: Boolean
	val returnSampleSize:Integer

  def updateSchema(s:String): Unit ={
    schema = s
  }

}

case class SparkResponseParameter(override  val className: String, val response: String) extends UserParameter{
	override def toString=response
}
case class CsvIngestionParameter(override  val className: String, override  val  filePath:String, override  val tableName : String, override val templateTableName:String, override  var schema : String, hasHeader : Boolean, returnSampleSize: Integer) extends IngestionParameter
case class xmlIngestionParameter(override  val className: String, override  val  filePath:String, override  val tableName : String , override val templateTableName:String, override  var schema : String, hasHeader : Boolean, rowTag: String, rootTag:String, returnSampleSize: Integer) extends IngestionParameter

case class CopybookIngestionParameter(    //code 1
																		 override  val className: String,
                                          conn:String,
																			 cpyBookName : String,
																			 cpyBookHdfsPath : String,
																			 dataFileHdfsPath: String = "",
																			 cpybookFont: String = "cp037",
																			 fileStructure: String = "FixedLength",
																			 binaryFormat: String = "FMT_MAINFRAME",
																			 splitOptoin: String = "SplitNone"
																		 )  extends UserParameter

case class JarParamter(override  val className: String, hdfsPaths:String)  extends UserParameter

case class ScalaSourceParameter(override val className : String, srcHdfsPath: String, owner:String, reportName: String, filenames: String)  extends UserParameter
case class ScalaTextParameter(override val className : String, sources: java.util.List[String])  extends UserParameter


case class QueryParameter(override val className: String, sql:String) extends UserParameter


trait UserClassRunner{
	def run () : Any
}
