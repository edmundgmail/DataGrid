package com.ddp.jarmanager

import java.io.{BufferedInputStream, File}

import org.apache.hadoop
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.xeustechnologies.jcl.{JarClassLoader, JclObjectFactory}
import com.ddp.access.JarParamter
/**
  * Created by cloudera on 9/3/16.
  */


case class JarLoader (jclFactory : JclObjectFactory, jcl: JarClassLoader, jarParamter: JarParamter) {

  val pathArray = jarParamter.hdfsPaths.split(":")
  val conf = new hadoop.conf.Configuration
  conf.set("fs.defaultFS", "com.ddp.rest.defaultFS")
  val fs = FileSystem.get (conf)
  for(p<-pathArray){
    val inputStream = new BufferedInputStream (fs.open (new Path( p) ) )
    jcl.add(inputStream)
  }
}

//add the classes under a folder
case class ClassLoader(jcl:JarClassLoader, folder : File){

  def run() = {
    recursiveListFiles(folder).filter(_.isFile).filter(_.getName.endsWith(".class")).foreach(jcl.add)
  }


  private def recursiveListFiles(file:File): Array[File] = {
    val these = file.listFiles
    these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }
}
