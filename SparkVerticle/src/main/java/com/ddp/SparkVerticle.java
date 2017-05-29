package com.ddp;

import com.ddp.access.*;
import com.ddp.cpybook.CopybookIngestion;
import com.ddp.ingestion.FileIngestionEngine;
import com.ddp.jarmanager.JarLoader;
import com.ddp.jarmanager.ScalaSourceCompiiler;
import com.ddp.userclass.Query;
import com.ddp.userclass.RunUserClass;
import com.ddp.util.*;
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
import java.util.Properties;

import static com.ddp.util.ClassUtils.findClass;

import org.datanucleus.util.StringUtils;
import org.xeustechnologies.jcl.JarClassLoader;
import org.xeustechnologies.jcl.JclObjectFactory;

/**
 * Created by cloudera on 2/8/17.
 */

public class SparkVerticle extends AbstractVerticle{

    private static Logger LOG = LoggerFactory.getLogger("SparkVerticle");


    private static io.vertx.kafka.client.consumer.KafkaConsumer<String, BaseRequest> consumer;
    private static io.vertx.kafka.client.producer.KafkaProducer<String, BaseRequest> producer;

    private static String sparkAppName;
    private static String sparkMaster;
    private static String consumerTopic;
    private static String producerTopic;
    private static String kafkaBrokers;
    private static String groupId;

    private static ScalaSourceCompiiler scalaSourceCompiiler;
    private static JarLoader jarLoader;
    private static RunUserClass runUserClass;
    private static SparkSession sparkSession;
    private static Query queryEngine;
    private static CopybookIngestion copybookIngestion;
    private static FileIngestionEngine fileIngestionEngine;

    private static UserParameterDeserializer userParameterDeserializer = UserParameterDeserializer.getInstance();
    private static Gson gson = new GsonBuilder().registerTypeAdapter(UserParameter.class, userParameterDeserializer).create();

    private static final JclObjectFactory jclFactory = JclObjectFactory.getInstance();
    private static final JarClassLoader jcl =new JarClassLoader();

    private static void initConfig(JsonObject js){
        sparkAppName = js.getString("spark.appname");
        sparkMaster = js.getString("spark.master");
        consumerTopic = js.getString("in.topic");
        producerTopic = js.getString("out.topic");
        kafkaBrokers = js.getString("kafka.brokers");
        groupId = js.getString("group.id");
    }

    private static  boolean hiveClassesArePresent() {
        try {
            SparkVerticle.class.forName("org.apache.spark.sql.hive.HiveSessionState");
            SparkVerticle.class.forName("org.apache.hadoop.hive.conf.HiveConf");
            return true;
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            return false;
        }
    }

    private static void createEngines()
    {
        queryEngine = Query.apply(sparkSession);
        runUserClass = RunUserClass.apply(jclFactory, jcl);
        jarLoader = JarLoader.apply(jclFactory, jcl);
        scalaSourceCompiiler = ScalaSourceCompiiler.apply(jclFactory, jcl);
        copybookIngestion = CopybookIngestion.apply(sparkSession.sqlContext());
        fileIngestionEngine = FileIngestionEngine.apply(sparkSession.sqlContext());
    }

    private static Object createSparkSession() {
        Object sparkSession;
        File outputDir = null;

        SparkConf conf = new SparkConf();
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
        /*
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

    private static io.vertx.kafka.client.consumer.KafkaConsumer<String, BaseRequest> createConsumerJava(Vertx vertx) {
        // creating the consumer using properties config
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BaseRequestDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        // use consumer for interacting with Apache Kafka
        io.vertx.kafka.client.consumer.KafkaConsumer<String, BaseRequest> consumer = io.vertx.kafka.client.consumer.KafkaConsumer.create(vertx, config);
        return consumer;
    }

    private static io.vertx.kafka.client.producer.KafkaProducer<String, BaseRequest> createProducerJava(Vertx vertx) {
        // creating the producer using map and class types for key and value serializers/deserializers
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
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
                        initConfig(js);
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
        sparkSession = (SparkSession) createSparkSession();
        createEngines();

        consumer = createConsumerJava(vertx);
        producer = createProducerJava(vertx);

        consumer.handler(record -> {
            try {
                if(record.value()!=null && record.value().sessionKey()!=0 && record.value().parameter()!=null){
                    LOG.info(record.value());
                    handleEvent(record.value());

                }
                ;
            }catch (Exception e)
            {
                e.printStackTrace();
            }
        });


        consumer.subscribe(consumerTopic);
    }

    private JsonObject formatResult(BaseRequest request, Object result){
        JsonObject ret = new JsonObject();
        ret.put("result", result);
        ret.put("sessionKey", request.sessionKey());
        return ret;
    }

    private void handleEvent(BaseRequest msg) {
        SparkSession spark = (SparkSession) sparkSession;

        if (msg.parameter().className().equals(CopybookIngestionParameter.class.getCanonicalName())) {
            CopybookIngestionParameter a = (CopybookIngestionParameter) msg.parameter();

            vertx.executeBlocking(future -> {
                // Call some blocking API that takes a significant amount of time to return
                Object result = copybookIngestion.run(a);
                future.complete(formatResult(msg,result));
            }, res -> {
                System.out.println("The result is: " + res.result());
            });
        }
        else if(msg.parameter().className().equals(CsvIngestionParameter.class.getCanonicalName())){
            CsvIngestionParameter a = (CsvIngestionParameter)msg.parameter();

            vertx.executeBlocking(future -> {
                // Call some blocking API that takes a significant amount of time to return
                //StructType schema = buildStructType(a.schema());
                Object result = fileIngestionEngine.ingestCsv(a);
                future.complete(result);
            }, res -> {
                System.out.println("The result is: " + res.result());
                String response = "";
                if(res.result()!=null){
                    response = res.result().toString();
                }
                UserParameter parameter = SparkResponseParameter.apply(SparkResponseParameter.class.getCanonicalName(), response);
                BaseRequest request = new BaseRequest(msg.sessionKey(), parameter,false);

                KafkaProducerRecord<String, BaseRequest> feedback = KafkaProducerRecord.create(producerTopic, request);
                producer.write(feedback);
            });

        }
        else if(msg.parameter().className().equals(xmlIngestionParameter.class.getCanonicalName())){
            xmlIngestionParameter a = (xmlIngestionParameter)msg.parameter();

            vertx.executeBlocking(future -> {
                // Call some blocking API that takes a significant amount of time to return
                Object result = fileIngestionEngine.ingestXml(a);
                future.complete(formatResult(msg,result));
            }, res -> {
                System.out.println("The result is: " + res.result());
            });

        }
        else if(msg.parameter().className().equals(ScalaSourceParameter.class.getCanonicalName())){
            ScalaSourceParameter a = (ScalaSourceParameter)msg.parameter();

            vertx.executeBlocking(future -> {
                // Call some blocking API that takes a significant amount of time to return
                Object result = scalaSourceCompiiler.compile(a);
                future.complete(formatResult(msg,result));
            }, res -> {
                System.out.println("The result is: " + res.result());
            });

        }
        else if(msg.parameter().className().equals(JarParamter.class.getCanonicalName())){
            JarParamter a = (JarParamter)msg.parameter();

            vertx.executeBlocking(future -> {
                // Call some blocking API that takes a significant amount of time to return
                Object result = jarLoader.load(a);
                future.complete(formatResult(msg,result));
            }, res -> {
                System.out.println("The result is: " + res.result());
            });
        }
        else if(msg.parameter().className().equals(UserClassParameter.class.getCanonicalName())){

            UserClassParameter a = (UserClassParameter)msg.parameter();

                vertx.executeBlocking(future -> {
                    Object result;
                    // Call some blocking API that takes a significant amount of time to return
                    if(!a.useSpark())
                        result = runUserClass.run(a);
                    else
                        result = runUserClass.runSpark(spark, a);
                    future.complete(formatResult(msg,result));
                }, res -> {
                    System.out.println("The result is: " + res.result());
                });
         }
        else if(msg.parameter().className().equals(QueryParameter.class.getCanonicalName())){
            QueryParameter a = (QueryParameter)msg.parameter();
            vertx.executeBlocking(future -> {
                queryEngine.query(a.sql());
            }, res -> {
                System.out.println("The result is: " + res.result());
            });
        }
    }
}
