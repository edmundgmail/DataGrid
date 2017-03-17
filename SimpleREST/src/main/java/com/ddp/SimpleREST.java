/*
 * Copyright 2014 Red Hat, Inc.
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  and Apache License v2.0 which accompanies this distribution.
 *
 *  The Eclipse Public License is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 *
 *  The Apache License v2.0 is available at
 *  http://www.opensource.org/licenses/apache2.0.php
 *
 *  You may elect to redistribute this code under either of these licenses.
 */

package com.ddp;

import com.codahale.metrics.MetricRegistryListener;
import com.ddp.access.*;
import com.ddp.hierarchy.DataBrowse;
import com.ddp.hierarchy.IDataBrowse;
import com.google.common.base.Strings;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.util.*;
import java.util.function.Consumer;

import com.ddp.util.*;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.kafka.client.producer.RecordMetadata;
import io.vertx.rxjava.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import rx.Observable;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
 public class SimpleREST extends AbstractVerticle {

  private Map<String, JsonObject> products = new HashMap<>();

  private IDataBrowse dataBrowse;
  private Logger LOGGER = LoggerFactory.getLogger("SimpleREST");
  private JDBCClient client;
  private KafkaProducer<String, BaseRequest> producer;
    private io.vertx.kafka.client.consumer.KafkaConsumer<String, BaseRequest> consumer;

    private UserParameterDeserializer userParameterDeserializer;
  private Gson gson;

 // public static void main(String argv[]){
        //Runner.runExample(SimpleREST.class);
    //}


  public static void main(String argv[]){
      Vertx vertx = Vertx.vertx();
      final JsonObject js = new JsonObject();
      vertx.fileSystem().readFile("app-conf.json", result -> {

          if (result.succeeded()) {
              Buffer buff = result.result();
              js.mergeIn(new JsonObject(buff.toString()));
              DeploymentOptions options = new DeploymentOptions().setConfig(js).setMaxWorkerExecuteTime(5000).setWorker(true).setWorkerPoolSize(30);
              vertx.deployVerticle(SimpleREST.class.getName(), options);
          } else {
              System.err.println("Oh oh ..." + result.cause());
          }
      });

  }

    @Override
  public void start() {

    setUpInitialData();

      Router router = Router.router(vertx);
      router.route().handler(
                CorsHandler.create("*")
                        .allowedMethod(HttpMethod.GET)
                        .allowedMethod(HttpMethod.POST)
                        .allowedMethod(HttpMethod.OPTIONS)
                        .allowedHeader("X-PINGARUNER")
                        .allowedHeader("Content-Type")
              );
      //router.route().handler(BodyHandler.create());
      router.get("/hierarchy").handler(this::getListHierarchy);

      router.post("/ingestion").handler(this::postIngestion);

      router.post("/runUserClass").handler(this::postRunUserClass);

      //router.route("/*").handler(StaticHandler.create());

      vertx.createHttpServer().requestHandler(router::accept).listen(config().getInteger("http.port", 9001));

        /*consumer.handler( record -> {
            System.out.println("Processing record=" + record.key() +  record.value());
        });

        consumer.subscribe("topic123");
        */

        userParameterDeserializer = new UserParameterDeserializer();

        userParameterDeserializer.registerDataType(CopybookIngestionParameter.class.getName(), CopybookIngestionParameter.class);
        userParameterDeserializer.registerDataType(JarParamter.class.getName(), JarParamter.class);
        userParameterDeserializer.registerDataType(ScalaSourceParameter.class.getName(), ScalaSourceParameter.class);

        gson = new GsonBuilder().registerTypeAdapter(UserParameter.class, userParameterDeserializer).create();


    }



    private void postIngestion(RoutingContext routingContext){
        // Custom message

        HttpServerResponse response = routingContext.response();
        Consumer<Integer> errorHandler = i-> response.setStatusCode(i).end();
        Consumer<String> responseHandler = s-> response.putHeader("content-type", "application/json").end(s);

        routingContext.request().bodyHandler(new Handler<Buffer>() {
            @Override
            public void handle(Buffer buffer) {
                LOGGER.info("buffer=" + buffer.toString());
                BaseRequest request = gson.fromJson(buffer.toString(), BaseRequest.class);
                LOGGER.info("request=" + request);
                KafkaProducerRecord<String, BaseRequest> record = KafkaProducerRecord.create("topic123", request);

                producer.write(record, done -> {
                        System.out.println("Message " + record.value()+ done.toString());

                });

                JsonObject o = new JsonObject();
                o.put("key","test this is");
                responseHandler.accept(o.encode());

                /*
                eventBus.send(config().getString("eventbus.spark"), clusterWideMessage, reply -> {
                    if (reply.succeeded()) {
                        System.out.println("Received reply: ");
                    } else {
                        System.out.println("No reply from cluster receiver");
                    }
                });*/

            }
        });

    }

    private void postRunUserClass(RoutingContext routingContext){
        // Custom message
        routingContext.request().bodyHandler(new Handler<Buffer>() {
            @Override
            public void handle(Buffer buffer) {

                CustomMessage clusterWideMessage = new CustomMessage(2, "", buffer.toString());

                /*eventBus.send(config().getString("eventbus.spark"), clusterWideMessage, reply -> {
                    if (reply.succeeded()) {
                        System.out.println("Received reply: ");
                    } else {
                        System.out.println("No reply from cluster receiver");
                    }
                });*/

            }
        });
    }


    private void getListHierarchy(RoutingContext routingContext){
    HttpServerResponse response = routingContext.response();
    Consumer<Integer> errorHandler = i-> response.setStatusCode(i).end();
    Consumer<String> responseHandler = s-> response.putHeader("content-type", "application/json").end(s);

    int pageNum = NumberUtils.toInt(routingContext.request().getParam("pageNum"), 0);
    int pageSize = NumberUtils.toInt(routingContext.request().getParam("pageSize"), 20);
    Long sourceID = NumberUtils.toLong(routingContext.request().getParam("sourceID"), 0);
    Long entityID = NumberUtils.toLong(routingContext.request().getParam("entityID"),0);

    dataBrowse.handleListHierarchy(errorHandler, responseHandler, pageNum, pageSize, sourceID, entityID);

}

    private io.vertx.kafka.client.consumer.KafkaConsumer<String, BaseRequest> createConsumerJava(Vertx vertx) {

        // creating the consumer using properties config
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BaseRequestDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "my_group4");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // use consumer for interacting with Apache Kafka
        io.vertx.kafka.client.consumer.KafkaConsumer<String, BaseRequest> consumer = io.vertx.kafka.client.consumer.KafkaConsumer.create(vertx, config);
        return consumer;
    }

    private  KafkaProducer<String, BaseRequest> createProducerJava(Vertx vertx) {

        // creating the producer using map and class types for key and value serializers/deserializers
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, com.ddp.util.BaseRequestSerializer.class);

        // use producer for interacting with Apache Kafka
        KafkaProducer<String, BaseRequest> producer = KafkaProducer.create(vertx, config, String.class, BaseRequest.class);
        return producer;
    }


    private void setUpInitialData() {
     final JDBCClient client = JDBCClient.createShared(vertx, new JsonObject()
             .put("url", "jdbc:mysql://localhost:3306/metadata_ddp?user=ddp&password=password")
             .put("driver_class", "com.mysql.jdbc.Driver")
             .put("max_pool_size", 30));

     dataBrowse = new DataBrowse(client);
     producer = createProducerJava(vertx);
     consumer = createConsumerJava(vertx);
        consumer.handler(record -> {
            try {
                LOGGER.info(record.value());
                LOGGER.info(record.key());
            }catch (Exception e)
            {
                e.printStackTrace();
            }
        });
        consumer.subscribe("topic456");

     //eventBus = getVertx().eventBus();
     //eventBus.registerDefaultCodec(CustomMessage.class, new CustomMessageCodec());

  }


}
