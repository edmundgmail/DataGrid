package com.ddp.utils

import java.io.FileInputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

/**
  * Created by eguo on 11/15/16.
  */
object Utils {
  def getTempPath = "/tmp/ddp/" + System.currentTimeMillis + "_" + util.Random.nextInt(10000) + ".tmp"

  def getHdfsConf = {
    val conf: Configuration = new Configuration
    conf.addResource(new FileInputStream("/etc/hadoop/conf/core-site.xml"))
    conf.addResource(new FileInputStream("/etc/hadoop/conf/hdfs-site.xml"))
    conf
  }

  def getHdfs= {
    FileSystem.get(getHdfsConf)
  }
}
