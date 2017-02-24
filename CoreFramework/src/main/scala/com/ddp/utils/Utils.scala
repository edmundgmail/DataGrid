package com.ddp.utils

/**
  * Created by eguo on 11/15/16.
  */
object Utils {
  def getTempPath() = "/tmp/ddp/" + System.currentTimeMillis + "_" + util.Random.nextInt(10000) + ".tmp"
}
