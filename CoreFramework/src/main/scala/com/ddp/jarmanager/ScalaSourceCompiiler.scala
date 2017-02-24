package com.ddp.jarmanager

import java.io._
import java.util.jar.{Attributes, JarEntry, JarOutputStream}

import com.twitter.util.Eval
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.xeustechnologies.jcl.{JarClassLoader, JclObjectFactory}
import org.apache.commons.io.filefilter.{FalseFileFilter, HiddenFileFilter, TrueFileFilter}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.hadoop

import scala.collection.JavaConversions.asScalaIterator
import com.ddp.access.ScalaSourceParameter

/**
  * Created by cloudera on 9/3/16.
  */


case class ScalaSourceCompiiler (jclFactory : JclObjectFactory, jcl: JarClassLoader, sourceFiles: ScalaSourceParameter) {

  def run():Unit = {

    val  targetDir = new File("target_" + System.currentTimeMillis + "_" + util.Random.nextInt(10000) + ".tmp")

    targetDir.mkdir

    val eval = new Eval(Some(targetDir))

    val pathArray = sourceFiles.srcHdfsPath.split(":")
    val conf = new hadoop.conf.Configuration
    conf.set("fs.defaultFS", "com.ddp.rest.defaultFS")
    val fs = FileSystem.get (conf)
    for(p<-pathArray){
      val inputStream = new BufferedInputStream (fs.open (new Path( p) ) )
      eval.compile(IOUtils.toString(inputStream, "UTF-8"))
      inputStream.close()
    }

    val jarFile = CreateJarFile.mkJar(targetDir, "Main")
    jcl.add(jarFile)

    FileUtils.forceDelete(targetDir)
    FileUtils.forceDelete(new File(jarFile))
  }

}
