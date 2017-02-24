package com.ddp.cpybook

import java.io.{BufferedInputStream, ByteArrayInputStream}
import java.nio.charset.StandardCharsets

import net.sf.JRecord.External.{CobolCopybookLoader, ExternalRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.concurrent.ExecutionContext
import scala.util.Try

/**
  * Created by cloudera on 8/30/16.
  */
object CopybookHelper {
  val copybookInt = new CobolCopybookLoader ()

  def getExternalRecordGivenCopybook(cbName : String, copybook : String, cbSplitOption : String, cbBinaryFormat: String, cbFileStructure:String, cbFont : String) : ExternalRecord = {
    val intCbSplitOptoin = Try(Constants.CopybookSplitOptionValues withName cbSplitOption).get.id
    val intCbBinaryFormat = Try(Constants.CopybookBinaryformatValues withName cbBinaryFormat).get.id
    val intCbFileStructure = Try(Constants.CopybookFileStructureValues withName cbFileStructure).get.id
    val externalRecord = copybookInt
      .loadCopyBook(new ByteArrayInputStream(copybook.getBytes(StandardCharsets.UTF_8)), cbName, intCbSplitOptoin, 0,
        cbFont, intCbBinaryFormat, 0, null)
    externalRecord.setFileStructure(intCbFileStructure)
  }

  def getExternalRecord(config: Configuration) : ExternalRecord = {

    val cbName = config.get(Constants.CopybookName)
    val cblPath = config.get (Constants.CopybookHdfsPath)
    val cbSplitOptoin = Try(Constants.CopybookSplitOptionValues withName config.get(Constants.CopybookSplitOpiton)).get.id
    val cbBinaryFormat = Try(Constants.CopybookBinaryformatValues withName config.get(Constants.CopybookBinaryformat)).get.id
    val cbFileStructure = Try(Constants.CopybookFileStructureValues withName config.get(Constants.CopybookFileStructure)).get.id
    val cbFont = config.get(Constants.CopybookFont)
    val fs = FileSystem.get (config)

    val inputStream = new BufferedInputStream (fs.open (new Path ( cblPath) ) )



    try {
      val externalRecord = copybookInt
        .loadCopyBook(inputStream, cbName, cbSplitOptoin, 0,
          cbFont, cbBinaryFormat, 0, null)
      externalRecord.setFileStructure(cbFileStructure)
    }
    catch{
      case e: Exception => {e.printStackTrace(); null}
    }

  }

}
