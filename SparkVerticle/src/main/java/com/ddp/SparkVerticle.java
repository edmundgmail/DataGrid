package com.ddp;

import com.ddp.access.*;
import com.ddp.cpybook.CopybookIngestion;
import com.ddp.jarmanager.JarLoader;
import com.ddp.jarmanager.ScalaSourceCompiiler;
import com.ddp.userclass.Query;
import com.ddp.userclass.RunUserClass;
import com.ddp.util.*;
import com.ddp.utils.Utils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;


import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Properties;

import static com.ddp.util.ClassUtils.findClass;
import org.xeustechnologies.jcl.JarClassLoader;
import org.xeustechnologies.jcl.JclObjectFactory;

/**
 * Created by cloudera on 2/8/17.
 */

public class SparkVerticle extends AbstractVerticle{

    private static Logger LOG = LoggerFactory.getLogger("SparkVerticle");

    private Gson gson = new Gson();
    private io.vertx.kafka.client.consumer.KafkaConsumer<String, BaseRequest> consumer;
    private io.vertx.kafka.client.producer.KafkaProducer<String, BaseRequest> producer;
    private static File outputDir;

    private UserParameterDeserializer userParameterDeserializer;
    private static Object sparkSession;    // spark 2.x
    private static final JclObjectFactory jclFactory = JclObjectFactory.getInstance();
    private static final JarClassLoader jcl =new JarClassLoader();

    private static String sparkAppName;
    private static String sparkMaster;

    /*
    private Object createSparkSession() {
        conf = new SparkConf().setMaster(config().getString("spark.master")).setAppName(config().getString("spark.appname"));

        System.setProperty("hive.metastore.uris", "thrift://quickstart.cloudera:9083");
        System.setProperty("hive.metastore.warehouse.dir", "hdfs:/user/hive/warehouse");
        System.setProperty("hive.metastore.execute.setugi", "true");

        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();

        return sparkSession;
    }*/

    private static  boolean hiveClassesArePresent() {
        try {
            SparkVerticle.class.forName("org.apache.spark.sql.hive.HiveSessionState");
            SparkVerticle.class.forName("org.apache.hadoop.hive.conf.HiveConf");
            return true;
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            return false;
        }
    }

    private static Object createSparkSession() {
        outputDir = null;//new File(Utils.getTempPath());

        //System.setProperty("hive.metastore.uris", "thrift://quickstart.cloudera:9083");
        //System.setProperty("hive.metastore.warehouse.dir", "hdfs:/user/hive/warehouse");
        //System.setProperty("hive.metastore.execute.setugi", "true");

        //SPARK_EXECUTOR_URI,SPARK_HOME

        SparkConf conf = new SparkConf();
        //loadClass(spark,"/usr/lib/hive/lib/hive-contrib.jar");
        //String [] jars = {"/usr/lib/hive/lib/hive-contrib.jar"};
        //conf.setJars(jars);
        LOG.info("------ Create new SparkContext {} -------", sparkMaster);
        String execUri = System.getenv("SPARK_EXECUTOR_URI");
        conf.setAppName(sparkAppName);

        if (outputDir != null) {
            conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath());
        }

        if (execUri != null) {
            conf.set("spark.executor.uri", execUri);
        }

        if (System.getenv("SPARK_HOME") != null) {
            conf.setSparkHome(System.getenv("SPARK_HOME"));
        }

        conf.set("spark.scheduler.mode", "FAIR");
        conf.setMaster(sparkMaster);

        boolean isYarnMode = false;
        if (isYarnMode) {
            conf.set("master", "yarn");
            conf.set("spark.submit.deployMode", "client");
        }

        /*Properties intpProperty = getProperty();

        for (Object k : intpProperty.keySet()) {
            String key = (String) k;
            String val = toString(intpProperty.get(key));
            if (key.startsWith("spark.") && !val.trim().isEmpty()) {
                logger.debug(String.format("SparkConf: key = [%s], value = [%s]", key, val));
                conf.set(key, val);
            }
        }

        setupConfForPySpark(conf);
        setupConfForSparkR(conf);
        */
        Class SparkSession = ClassUtils.findClass("org.apache.spark.sql.SparkSession");
        Object builder = ClassUtils.invokeStaticMethod(SparkSession, "builder");
        ClassUtils.invokeMethod(builder, "config", new Class[]{ SparkConf.class }, new Object[]{ conf });

        boolean useHiveContext = true;
        if (useHiveContext) {
            if (hiveClassesArePresent()) {
                ClassUtils.invokeMethod(builder, "enableHiveSupport");
                sparkSession = ClassUtils.invokeMethod(builder, "getOrCreate");
                LOG.info("Created Spark session with Hive support");
            } else {
                ClassUtils.invokeMethod(builder, "config",
                        new Class[]{ String.class, String.class},
                        new Object[]{ "spark.sql.catalogImplementation", "in-memory"});
                sparkSession = ClassUtils.invokeMethod(builder, "getOrCreate");
                LOG.info("Created Spark session with Hive support use in-memory catalogImplementation");
            }
        } else {
            sparkSession = ClassUtils.invokeMethod(builder, "getOrCreate");
            LOG.info("Created Spark session");
        }

        return sparkSession;
    }

    private io.vertx.kafka.client.consumer.KafkaConsumer<String, BaseRequest> createConsumerJava(Vertx vertx) {

        // creating the consumer using properties config
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BaseRequestDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "my_group3");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // use consumer for interacting with Apache Kafka
        io.vertx.kafka.client.consumer.KafkaConsumer<String, BaseRequest> consumer = io.vertx.kafka.client.consumer.KafkaConsumer.create(vertx, config);
        return consumer;
    }

    private io.vertx.kafka.client.producer.KafkaProducer<String, BaseRequest> createProducerJava(Vertx vertx) {

        // creating the producer using map and class types for key and value serializers/deserializers
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, com.ddp.util.BaseRequestSerializer.class);

        // use producer for interacting with Apache Kafka
        io.vertx.kafka.client.producer.KafkaProducer<String, BaseRequest> producer = io.vertx.kafka.client.producer.KafkaProducer.create(vertx, config, String.class, BaseRequest.class);
        return producer;
    }

    public static void main(String argv[]) {
        //DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(js).setMaxWorkerExecuteTime(5000).setWorker(true).setWorkerPoolSize(30);

        VertxOptions options = new VertxOptions().setBlockedThreadCheckInterval(200000000);
        options.setClustered(true);

        Vertx.clusteredVertx(options, res -> {
            if (res.succeeded()) {
                Vertx vertx = res.result();
                final JsonObject js = new JsonObject();
                vertx.fileSystem().readFile("app-conf.json", result -> {
                    if (result.succeeded()) {
                        Buffer buff = result.result();
                        js.mergeIn(new JsonObject(buff.toString()));
                        sparkAppName = js.getString("spark.appname");
                        sparkMaster = js.getString("spark.master");

                        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(js).setMaxWorkerExecuteTime(5000).setWorker(true).setWorkerPoolSize(5);
                        vertx.deployVerticle(SparkVerticle.class.getName(), deploymentOptions);
                    } else {
                        System.err.println("Oh oh ..." + result.cause());
                    }
                });

            }
        });
    }

    public static SparkSession getSparkSession(){
        return (SparkSession) sparkSession;
    }

    public void start() {

        UserParameterDeserializer userParameterDeserializer = UserParameterDeserializer.getInstance();

        Gson gson = new GsonBuilder().registerTypeAdapter(UserParameter.class, userParameterDeserializer).create();
        createSparkSession();

        consumer = createConsumerJava(vertx);
        producer = createProducerJava(vertx);


        consumer.handler(record -> {
            try {
                if(record.value()!=null && record.value().sessionKey()!=0 && record.value().parameter()!=null){
                    LOG.info(record.value());
                    handleEvent(record.value());
                    BaseRequest request = new BaseRequest(456, null);

                    KafkaProducerRecord<String, BaseRequest> feedback = KafkaProducerRecord.create("topic456", request);
                    producer.write(feedback);
                }
                ;
            }catch (Exception e)
            {
                e.printStackTrace();
            }
        });


        consumer.subscribe("topic123");
        BaseRequest request = new BaseRequest(999, null);


    }

    private void loadClass(SparkSession spark, String path){
        try {
            File file = new File(path);
            URL url = file.toURI().toURL();

            URLClassLoader classLoader = (URLClassLoader)spark.getClass().getClassLoader();

            Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
            method.setAccessible(true);
            method.invoke(classLoader, url);

            spark.getClass().forName("org.apache.hadoop.hive.contrib.serde2.RegexSerDe");

        } catch (Exception ex) {
            LOG.error(ex.getMessage());
            ex.printStackTrace();
        }
    }

    private void handleEvent(BaseRequest msg) {
        SparkSession spark = (SparkSession) sparkSession;

        if (msg.parameter().className().equals(CopybookIngestionParameter.class.getCanonicalName())) {
            CopybookIngestionParameter a = (CopybookIngestionParameter) msg.parameter();

            vertx.executeBlocking(future -> {
                // Call some blocking API that takes a significant amount of time to return
                Object result = CopybookIngestion.apply(spark.sqlContext(), a).run();
                future.complete(result);
            }, res -> {
                System.out.println("The result is: " + res.result());
            });
        }
        else if(msg.parameter().className().equals(ScalaSourceParameter.class.getCanonicalName())){
            ScalaSourceParameter a = (ScalaSourceParameter)msg.parameter();

            vertx.executeBlocking(future -> {
                // Call some blocking API that takes a significant amount of time to return
                Object result = ScalaSourceCompiiler.apply(jclFactory, jcl, a).run();
                future.complete(result);
            }, res -> {
                System.out.println("The result is: " + res.result());
            });

        }
        else if(msg.parameter().className().equals(JarParamter.class.getCanonicalName())){
            JarParamter a = (JarParamter)msg.parameter();

            vertx.executeBlocking(future -> {
                // Call some blocking API that takes a significant amount of time to return
                Object result = JarLoader.apply(jclFactory, jcl, a);
                future.complete(result);
            }, res -> {
                System.out.println("The result is: " + res.result());
            });
        }
        else if(msg.parameter().className().equals(UserClassParameter.class.getCanonicalName())){

            UserClassParameter a = (UserClassParameter)msg.parameter();

                vertx.executeBlocking(future -> {
                    // Call some blocking API that takes a significant amount of time to return
                    if(!a.useSpark())
                        RunUserClass.apply(jclFactory, jcl, spark, a).run();
                    else
                        RunUserClass.apply(jclFactory, jcl, spark, a).runSpark();

                    //future.complete(result);
                }, res -> {
                    System.out.println("The result is: " + res.result());
                });
         }
        else if(msg.parameter().className().equals(QueryParameter.class.getCanonicalName())){

            //loadClass(spark,"/usr/lib/hive/lib/hive-contrib.jar");
            QueryParameter a = (QueryParameter)msg.parameter();


            vertx.executeBlocking(future -> {
                // Call some blocking API that takes a significant amount of time to return
                //Object result = RunUserClass.apply(jclFactory, jcl, sqlContext, a).run();
                //future.complete(result);
                Query.apply(spark).query(a.sql());
            }, res -> {
                System.out.println("The result is: " + res.result());
            });
        }
    }
}
