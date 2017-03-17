package com.ddp;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * Created by cloudera on 2/16/17.
 */
public class TestSparkContext {
    private Logger LOG = LoggerFactory.getLogger(TestSparkContext.class);

    public static void  main(String argv[]){
        TestSparkContext t = new TestSparkContext();


        SparkSession s = (SparkSession) t.createSparkSession();
        return;

    }

    private Object createSparkSession() {
        SparkConf conf = new SparkConf();
        LOG.info("------ Create new SparkContext {} -------");
        //String execUri = System.getenv("SPARK_EXECUTOR_URI");
        conf.setAppName("aaa");

        /*
        if (execUri != null) {
            conf.set("spark.executor.uri", execUri);
        }

        if (System.getenv("SPARK_HOME") != null) {
            conf.setSparkHome(System.getenv("SPARK_HOME"));
        }

        conf.set("spark.scheduler.mode", "FAIR");
        conf.setMaster("yarn");
        conf.set("master", "yarn");
        conf.set("spark.submit.deployMode", "client");
        //conf.set("spark.executor.uri", "local:/usr/lib/spark2/spark-archive.zip");
        conf.set("spark.yarn.jars","local:/usr/lib/spark2/spark-archive.zip");
        conf.set("spark.driver.memory", "1g");
        conf.set("spark.driver.cores", "1");
        conf.set("spark.blockManager.port", "38000");
        conf.set("spark.broadcast.port", "38001");
        conf.set("spark.driver.port", "38002");
        conf.set("spark.executor.port", "38003");
        conf.set("spark.fileserver.port", "38004");
        conf.set("spark.replClassServer.port", "38005");

*/
        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        return sparkSession;
    }
}
