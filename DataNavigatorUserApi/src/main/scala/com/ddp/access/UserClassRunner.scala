package com.ddp.access


trait UserParameter

case class UserClassParameter(userClassName:String) extends UserParameter  //code 2

case class CopybookIngestionParameter(    //code 1
																			 conn:String,
																			 cpyBookName : String,
																			 cpyBookHdfsPath : String,
																			 dataFileHdfsPath: String = "",
																			 cpybookFont: String = "cp037",
																			 fileStructure: String = "FixedLength",
																			 binaryFormat: String = "FMT_MAINFRAME",
																			 splitOptoin: String = "SplitNone"
																		 )  extends UserParameter

case class JarParamter(hdfsPaths:String)  extends UserParameter  //code 3
case class ScalaSourceParameter(srcHdfsPath: String)  extends UserParameter  //code4


trait UserClassRunner{
	def run () : Any 
}
