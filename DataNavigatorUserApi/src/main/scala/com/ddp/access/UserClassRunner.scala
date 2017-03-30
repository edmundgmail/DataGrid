package com.ddp.access


trait UserParameter {
	def className:String
}

case class BaseRequest(sessionKey : Long,  parameter: UserParameter)

case class UserClassParameter(override val className:String, userClassName:String, useSpark : Boolean = false) extends UserParameter

case class FileIngestionParameter( override  val className: String, format:String, filePath:String, tableName : String , Schema : String) extends UserParameter

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
