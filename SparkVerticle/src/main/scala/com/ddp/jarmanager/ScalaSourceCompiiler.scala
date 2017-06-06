package com.ddp.jarmanager

import java.io._

import com.ddp.ScriptLoader
import com.ddp.access.ScalaSourceParameter
import com.ddp.utils.Utils
import com.twitter.util.Eval
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop
import org.apache.hadoop.fs.{FileSystem, Path}
import org.xeustechnologies.jcl.{JarClassLoader, JclObjectFactory}

import scala.collection.JavaConversions._
import scala.tools.scalap.scalax.util.StringUtil

/**
  * Created by cloudera on 9/3/16.
  */


case class ScalaSourceCompiiler (jclFactory : JclObjectFactory, jcl: JarClassLoader) {

  def compile(sources : String): Unit ={
    if(StringUtils.isEmpty(sources)) {
    val  targetDir = new File("target_" + System.currentTimeMillis + "_" + util.Random.nextInt(10000) + ".tmp")

    targetDir.mkdir

    val eval = new Eval(Some(targetDir))

    eval.compile(sources)
    val jarFile = CreateJarFile.mkJar(targetDir, "Main")
    val clazzNames = JarEnumerator.getClazzNames(jarFile)

    for(p<-clazzNames){
      try{
        jcl.unloadClass(p)
      }
      catch{
        case e: Throwable => e.printStackTrace()
      }

    }

    jcl.add(jarFile)

    FileUtils.forceDelete(targetDir)
    FileUtils.forceDelete(new File(jarFile))
    }
    "Success"
  }

  def compile(sourceFiles: ScalaSourceParameter, scriptLoader: ScriptLoader):Any = {

    val  targetDir = new File("target_" + System.currentTimeMillis + "_" + util.Random.nextInt(10000) + ".tmp")

    targetDir.mkdir
    val eval = new Eval(Some(targetDir))

    val pathArray = sourceFiles.srcHdfsPath.split(":")
    val fs = Utils.getHdfs
    for(p<-pathArray){
      val inputStream = new BufferedInputStream (fs.open (new Path( p) ) )
      val source = IOUtils.toString(inputStream, "UTF-8")
      eval.compile(source)
      inputStream.close()
    }

    val jarFile = CreateJarFile.mkJar(targetDir, "Main")
    val clazzNames = JarEnumerator.getClazzNames(jarFile)

    for(p<-clazzNames){
        try{
          jcl.unloadClass(p)
        }
      catch{
        case e: Throwable => e.printStackTrace()
      }

    }

    jcl.add(jarFile)

    scriptLoader.loadReports(sourceFiles.owner, sourceFiles.reportName, sourceFiles.srcHdfsPath)

    FileUtils.forceDelete(targetDir)
    FileUtils.forceDelete(new File(jarFile))
    "Success"
  }

}
