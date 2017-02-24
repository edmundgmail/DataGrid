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

import com.ddp.hierarchy.DataBrowse;
import com.ddp.hierarchy.IDataBrowse;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.ddp.util.*;
import io.vertx.ext.web.handler.CorsHandler;
import org.apache.commons.lang3.math.NumberUtils;


/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class SimpleREST extends AbstractVerticle {

  private Map<String, JsonObject> products = new HashMap<>();

  private IDataBrowse dataBrowse;
  private Logger LOGGER = LoggerFactory.getLogger("SimpleREST");
  private JDBCClient client;
  private EventBus eventBus;

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
    }



    private void postIngestion(RoutingContext routingContext){
        // Custom message
        routingContext.request().bodyHandler(new Handler<Buffer>() {
            @Override
            public void handle(Buffer buffer) {

                CustomMessage clusterWideMessage = new CustomMessage(1, "", buffer.toString());
                eventBus.send(config().getString("eventbus.spark"), clusterWideMessage, reply -> {
                    if (reply.succeeded()) {
                        System.out.println("Received reply: ");
                    } else {
                        System.out.println("No reply from cluster receiver");
                    }
                });

            }
        });

    }

    private void postRunUserClass(RoutingContext routingContext){
        // Custom message
        routingContext.request().bodyHandler(new Handler<Buffer>() {
            @Override
            public void handle(Buffer buffer) {

                CustomMessage clusterWideMessage = new CustomMessage(2, "", buffer.toString());
                eventBus.send(config().getString("eventbus.spark"), clusterWideMessage, reply -> {
                    if (reply.succeeded()) {
                        System.out.println("Received reply: ");
                    } else {
                        System.out.println("No reply from cluster receiver");
                    }
                });

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

 private void setUpInitialData() {
     final JDBCClient client = JDBCClient.createShared(vertx, new JsonObject()
             .put("url", "jdbc:mysql://localhost:3306/metadata_ddp?user=ddp&password=password")
             .put("driver_class", "com.mysql.jdbc.Driver")
             .put("max_pool_size", 30));

     dataBrowse = new DataBrowse(client);
     eventBus = getVertx().eventBus();
     eventBus.registerDefaultCodec(CustomMessage.class, new CustomMessageCodec());

  }


}
