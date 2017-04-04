package com.ddp.access


trait UserParameter {
	val className:String
}

case class BaseRequest(sessionKey : Long,  parameter: UserParameter, needPadding: Boolean = false)

case class UserClassParameter(override val className:String, userClassName:String, useSpark : Boolean = false) extends UserParameter

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

case class csvIngestionParameter( override  val className: String,override  val  filePath:String, override  val tableName : String , override val templateTableName:String, override  var schema : String, hasHeader : Boolean, returnSampleSize: Integer) extends IngestionParameter
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

case class ScalaSourceParameter(override val className : String, srcHdfsPath: String)  extends UserParameter

case class QueryParameter(override val className: String, sql:String) extends UserParameter


trait UserClassRunner{
	def run () : Any
}
