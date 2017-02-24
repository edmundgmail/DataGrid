package com.ddp.cpybook

/**
  * Created by cloudera on 8/28/16.
  */
import java.io.{BufferedInputStream, IOException}

import net.sf.JRecord.Common.{BasicFileSchema, IBasicFileSchema}
import net.sf.JRecord.Details.AbstractLine
import net.sf.JRecord.External.ExternalRecord
import net.sf.JRecord.IO.{AbstractLineReader, LineIOProvider}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileSplit

import scala.util.Try


class CopybookRecordReader extends RecordReader[LongWritable, Map[String, String]] {


  var recordByteLength : Int = 0
  var start : Long = 0
  var pos : Long = 0
  var end: Long = 0

  var key : LongWritable = new io.LongWritable()
  var value:  Map[String, String]  = Map()
  var lr: AbstractLineReader = null
  var externalRecord : ExternalRecord = null


  @throws(classOf[IOException])
  @throws (classOf[InterruptedException])
  override def initialize ( split : InputSplit, context: TaskAttemptContext) : Unit = {
    val fileStructure = Try(Constants.CopybookFileStructureValues withName context.getConfiguration.get(Constants.CopybookFileStructure)).get.id

    externalRecord = CopybookHelper.getExternalRecord(context.getConfiguration)
    lr = LineIOProvider.getInstance().getLineReader(
      fileStructure,
      LineIOProvider.getInstance().getLineProvider(BasicFileSchema.newFixedSchema(fileStructure)))

    // jump to the point in the split that the first whole record of split
    // starts at
    val fileSplit = split.asInstanceOf[FileSplit]

    start = fileSplit.getStart()
    end = start + fileSplit.getLength()

   for (field <- externalRecord.getRecordFields) {
      recordByteLength += field.getLen
   }

    val fs: FileSystem = FileSystem.get(context.getConfiguration)
    val fileIn = new BufferedInputStream(fs.open(fileSplit
      .getPath()))

    if (start != 0) {
      pos = start - (start % recordByteLength) + recordByteLength
      fileIn.skip(pos)
    }

     lr.open(fileIn, externalRecord)
  }

  @throws(classOf[IOException])
  @throws (classOf[InterruptedException])
  override def  nextKeyValue () : Boolean = {

      if (pos > end) {
        return false;
      }
      val line : AbstractLine  = lr.read ()

      if (line == null) {
        return false;
      }

      pos += recordByteLength;

      key.set (pos);

      for ( field<- externalRecord.getRecordFields ()){
          value += field.getName->line.getFieldValue(field.getName).toString
      }
    return true
}

  @throws(classOf[IOException])
  @throws (classOf[InterruptedException])
  override def  getCurrentKey = key


  @throws(classOf[IOException])
  @throws (classOf[InterruptedException])
  override def getCurrentValue  = value

  @throws(classOf[IOException])
  @throws (classOf[InterruptedException])
  override def  getProgress () : Float = {
    if (start == end) {
     0.0f
    } else {
     Math.min (1.0f, (pos - start) / (end - start).asInstanceOf[Float] )
    }
  }

  @throws(classOf[IOException])
  override def close  () : Unit =  lr.close



}
