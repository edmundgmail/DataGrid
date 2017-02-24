package com.ddp.cpybook

import java.io.IOException

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

/**
  * Created by cloudera on 8/28/16.
  */

class CopybookInputFormat extends FileInputFormat[LongWritable, Map[String, String]]{

  @throws[IOException]
  @throws[InterruptedException]
  override def createRecordReader (split: InputSplit, context: TaskAttemptContext): RecordReader[LongWritable, Map[String, String]] = {
    // TODO Auto-generated method stub
    return new CopybookRecordReader
  }
}
