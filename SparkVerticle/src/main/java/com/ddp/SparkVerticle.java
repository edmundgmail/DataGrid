package com.ddp;

import com.ddp.access.*;
import com.ddp.cpybook.CopybookIngestion;
import com.ddp.util.CustomMessage;
import com.ddp.util.*;
import com.ddp.util.Runner;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.jersey.api.json.JSONConfiguration;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.avro.JsonProperties;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.*;


import java.util.*;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.parboiled.common.Tuple2;
import org.apache.commons.beanutils.PropertyUtils;
import scala.collection.JavaConversions;


/**
 * Created by cloudera on 2/8/17.
 */
public class SparkVerticle extends AbstractVerticle{

    private Logger LOG = LoggerFactory.getLogger("SparkVerticle");

    private SparkSession spark;

    private Gson gson = new Gson();
    private io.vertx.kafka.client.consumer.KafkaConsumer<String, BaseRequest> consumer;
    private io.vertx.kafka.client.producer.KafkaProducer<String, BaseRequest> producer;
    private SparkConf conf;


    private UserParameterDeserializer userParameterDeserializer;
    private Object createSparkSession() {

        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

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

    public static void main(String argv[]){
        Vertx vertx = Vertx.vertx();
        final JsonObject js = new JsonObject();
        vertx.fileSystem().readFile("app-conf.json", result -> {
            if (result.succeeded()) {
                Buffer buff = result.result();
                js.mergeIn(new JsonObject(buff.toString()));
                DeploymentOptions options = new DeploymentOptions().setConfig(js).setMaxWorkerExecuteTime(5000).setWorker(true).setWorkerPoolSize(30);
                vertx.deployVerticle(SparkVerticle.class.getName(), options);
            } else {
                System.err.println("Oh oh ..." + result.cause());
            }
        });

    }

    public void start() {
        UserParameterDeserializer userParameterDeserializer = new UserParameterDeserializer();

        userParameterDeserializer.registerDataType(CopybookIngestionParameter.class.getName(), CopybookIngestionParameter.class);
        userParameterDeserializer.registerDataType(JarParamter.class.getName(), JarParamter.class);
        userParameterDeserializer.registerDataType(ScalaSourceParameter.class.getName(), ScalaSourceParameter.class);

        Gson gson = new GsonBuilder().registerTypeAdapter(UserParameter.class, userParameterDeserializer).create();
        conf = new SparkConf().setMaster(config().getString("spark.master")).setAppName(config().getString("spark.appname"));
        spark = (SparkSession) createSparkSession();

        //JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(1));


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

    private void handleEvent(BaseRequest msg) {

        if (msg.parameter().className().equals(CopybookIngestionParameter.class.getCanonicalName())) {
            CopybookIngestionParameter a = (CopybookIngestionParameter) msg.parameter();

            vertx.executeBlocking(future -> {
                // Call some blocking API that takes a significant amount of time to return
                Object result = CopybookIngestion.apply(config().getString("hdfs.conf"), spark.sqlContext(), a).run();
                future.complete(result);
            }, res -> {
                System.out.println("The result is: " + res.result());
            });
        }
        else if(msg.parameter().className().equals(UserClassParameter.class.getCanonicalName())){

            UserClassParameter a = (UserClassParameter)msg.parameter();

                vertx.executeBlocking(future -> {
                    // Call some blocking API that takes a significant amount of time to return
                    //Object result = RunUserClass.apply(jclFactory, jcl, sqlContext, a).run();
                    //future.complete(result);
                }, res -> {
                    System.out.println("The result is: " + res.result());
                });
         }

    }
}
