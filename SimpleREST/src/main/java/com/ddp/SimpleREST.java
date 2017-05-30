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

import com.ddp.access.*;
import com.ddp.hierarchy.DataBrowse;
import com.ddp.hierarchy.IDataBrowse;
import com.ddp.utils.Utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.web.FileUpload;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.io.IOException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import com.ddp.util.*;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
 public class SimpleREST extends AbstractVerticle {
  private IDataBrowse dataBrowse;
  private Logger LOGGER = LoggerFactory.getLogger("SimpleREST");
  private JDBCClient client;

  private static String hdfsUploadHome;
  private static String localUploadHome;
    private static Integer httpPort;
    private static String sqlDriverClass;
    private static String sqlUrl;
    private static String toScheduleEvent;
    private static EventBus eventBus;
    private static UserParameterDeserializer userParameterDeserializer = UserParameterDeserializer.getInstance();
    private static Gson gson = new GsonBuilder().registerTypeAdapter(UserParameter.class, userParameterDeserializer).create();

    private static FileSystem fs = Utils.getHdfs();


    private static void initConfig(JsonObject js){
        httpPort = js.getInteger("http.port");
        sqlDriverClass = js.getString("driver.class");
        sqlUrl = js.getString("sql.url");
        localUploadHome =js.getString("local.upload.home");
        hdfsUploadHome =js.getString("hdfs.upload.home");
        toScheduleEvent = js.getString("eventbus.schedverticle");
    }


    public static void main(String argv[]){

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
                      vertx.deployVerticle(SimpleREST.class.getName(), deploymentOptions);
                  } else {
                      System.err.println("Oh oh ..." + result.cause());
                  }
              });

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
      router.post("/hierarchy").handler(this::postHierarchy);
      router.post("/runner").handler(this::postSparkRunner);

      router.post("/postJars").handler(BodyHandler.create()
              .setUploadsDirectory(localUploadHome));
      router.post("/postJars").handler(this::postJars);

      router.post("/postScalaFiles").handler(BodyHandler.create()
              .setUploadsDirectory(localUploadHome));
      router.post("/postScalaFiles").handler(this::postScalaFiles);

      router.post("/postSampleFiles").handler(BodyHandler.create()
              .setUploadsDirectory(localUploadHome));
      router.post("/postSampleFiles").handler(this::postSampleFiles);


      vertx.createHttpServer().requestHandler(router::accept).listen(httpPort);

      eventBus = getVertx().eventBus();
      eventBus.registerDefaultCodec(BaseRequest.class, new BaseRequestCodec());

    }

    private Map<String, FileUpload>  getUploadedFiles(RoutingContext ctx){
        // any number of uploads
        Map<String, FileUpload> files = new HashMap<>();
        for (FileUpload f : ctx.fileUploads()) {
            // do whatever you need to do with the file (it is already saved
            // on the directory you wanted...

            try{
                Path p = new Path(f.uploadedFileName());
                fs.copyFromLocalFile(p, new Path(hdfsUploadHome));
                files.put(hdfsUploadHome + "/" + p.getName(), f);
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        return files;

    }

    private void postSampleFiles(RoutingContext ctx){
        HttpServerResponse response = ctx.response();
        Consumer<Integer> errorHandler = i -> response.setStatusCode(i).end();
        Consumer<String> responseHandler = s -> response.putHeader("content-type", "application/json").end(s);

        Map<String, FileUpload> files = getUploadedFiles(ctx);

        FileUpload file1 = files.get(files.keySet().toArray()[0]);
        String name = file1.name();
        String tableName=file1.fileName();
        IngestionParameter parameter = null;
        if(name.equalsIgnoreCase("csv")){
            String hdfsPath = "";
            for(String s: files.keySet()){
                hdfsPath+= fs.getUri()+ s+",";
            }

            hdfsPath=hdfsPath.substring(0, hdfsPath.length()-1);

            parameter= CsvIngestionParameter.apply(CsvIngestionParameter.class.getCanonicalName(), hdfsPath, tableName, null, null,  false, 100);
        }

        BaseRequest baseRequest = BaseRequest.apply(123, parameter, false);

        sendToSpark(BaseConsumer.apply(baseRequest, responseHandler));

    }


    private void postScalaFiles(RoutingContext ctx){
        HttpServerResponse response = ctx.response();
        Consumer<Integer> errorHandler = i -> response.setStatusCode(i).end();
        Consumer<String> responseHandler = s -> response.putHeader("content-type", "application/json").end(s);

        Map<String, FileUpload> files = getUploadedFiles(ctx);
        ScalaSourceParameter scalaSourceParameter = ScalaSourceParameter.apply(ScalaSourceParameter.class.getCanonicalName(), String.join(":", files.keySet()));
        Random random = new Random();
        Integer sessionKey = random.nextInt(10000);
        BaseRequest request = BaseRequest.apply(sessionKey, scalaSourceParameter,false);

        LOGGER.info("request=" + request);
        sendToSpark(BaseConsumer.apply(request, responseHandler));
    }

    private void postJars(RoutingContext ctx){
        HttpServerResponse response = ctx.response();
        Consumer<Integer> errorHandler = i -> response.setStatusCode(i).end();
        Consumer<String> responseHandler = s -> response.putHeader("content-type", "application/json").end(s);

        // in your example you only handle 1 file upload, here you can handle
            // any number of uploads
            Map<String, FileUpload> jars = getUploadedFiles(ctx);

            JarParamter jarParamter = JarParamter.apply(JarParamter.class.getCanonicalName(), String.join(":", jars.keySet()));

            Random random = new Random();
            Integer sessionKey = random.nextInt(10000);
            BaseRequest request = BaseRequest.apply(sessionKey, jarParamter,false);

            LOGGER.info("request=" + request);

            sendToSpark(BaseConsumer.apply(request, responseHandler));
    }


    private static void sendToSpark(BaseConsumer baseConsumer ){
        eventBus.send("cluster-message-receiver", baseConsumer.baseRequest(), reply -> {
            if (reply.succeeded()) {
                BaseRequest replyMessage = (BaseRequest) reply.result().body();
                baseConsumer.responseHandler().accept(replyMessage.parameter().toString());
                System.out.println("Received reply: "+replyMessage.parameter());
            } else {
                System.out.println("No reply from cluster receiver");
            }
        });
    }

    BiConsumer<BaseConsumer, String> biConsumer = (request, padding)-> {
        IngestionParameter parameter = (IngestionParameter) request.baseRequest().parameter();
        parameter.updateSchema(padding);
        sendToSpark(request);
    };

    Function<BaseConsumer,Consumer<String>> currier = a -> b -> biConsumer.accept( a, b ) ;

    private void postHierarchy(RoutingContext routingContext){
        HttpServerResponse response = routingContext.response();
        Consumer<Integer> errorHandler = i -> response.setStatusCode(i).end();
        Consumer<String> responseHandler = s -> response.putHeader("content-type", "application/json").end(s);

        routingContext.request().bodyHandler(new Handler<Buffer>() {
            @Override
            public void handle(Buffer buffer) {
                LOGGER.info("buffer=" + buffer.toString());
                BaseRequest request = gson.fromJson(buffer.toString(), BaseRequest.class);
                NewDataSourceParameter newDataSourceParameter = (NewDataSourceParameter) request.parameter();

                dataBrowse.handleUpdateHierarchy(errorHandler, responseHandler, newDataSourceParameter);
            }
        });
    }

    private void postSparkRunnerCustomRequest(RoutingContext routingContext){
        HttpServerResponse response = routingContext.response();
        Consumer<Integer> errorHandler = i -> response.setStatusCode(i).end();
        Consumer<String> responseHandler = s -> response.putHeader("content-type", "application/json").end(s);
    }

    private void postSparkRunner(RoutingContext routingContext) {
        // Custom message

        HttpServerResponse response = routingContext.response();
        Consumer<Integer> errorHandler = i -> response.setStatusCode(i).end();
        Consumer<String> responseHandler = s -> response.putHeader("content-type", "application/json").end(s);

        routingContext.request().bodyHandler(new Handler<Buffer>() {
            @Override
            public void handle(Buffer buffer) {
                LOGGER.info("buffer=" + buffer.toString());
                BaseRequest request = gson.fromJson(buffer.toString(), BaseRequest.class);

                if (request.needPadding()) {
                    String className = request.parameter().className();
                    if (className.equals(CsvIngestionParameter.class.getCanonicalName()) ||
                            className.equals(xmlIngestionParameter.class.getCanonicalName())) {
                        IngestionParameter parameter = (IngestionParameter) request.parameter();
                        Consumer curried = currier.apply(BaseConsumer.apply(request, responseHandler));
                        dataBrowse.getEntityDetail(parameter.templateTableName(), curried);
                    }
                }
                else {
                    sendToSpark(BaseConsumer.apply(request, responseHandler));
                }
            }
        });
    }

    private void getListHierarchy(RoutingContext routingContext){
    HttpServerResponse response = routingContext.response();
    Consumer<Integer> errorHandler = i-> response.setStatusCode(i).end();
    Consumer<String> responseHandler = s-> response.putHeader("content-type", "application/json").end(s);

    int pageNum = NumberUtils.toInt(routingContext.request().getParam("pageNum"), 0);
    int pageSize = NumberUtils.toInt(routingContext.request().getParam("pageSize"), 10000);
    String level = routingContext.request().getParam("level");
    Long id = NumberUtils.toLong(routingContext.request().getParam("id"),0);

    dataBrowse.handleListHierarchy(errorHandler, responseHandler, pageNum, pageSize, level, id);

}

    private void setUpInitialData() {

     final JDBCClient client = JDBCClient.createShared(vertx, new JsonObject()
             .put("url", sqlUrl)
             .put("driver_class", sqlDriverClass)
             .put("max_pool_size", 30));

     dataBrowse = new DataBrowse(client);

     //eventBus = getVertx().eventBus();
     //eventBus.registerDefaultCodec(CustomMessage.class, new CustomMessageCodec());

  }


}
