package com.ddp;

import com.ddp.access.CopybookIngestionParameter;
import com.ddp.access.UserClassParameter;
import com.ddp.cpybook.CopybookIngestion;
import com.ddp.util.CustomMessage;
import com.ddp.util.CustomMessageCodec;
import com.ddp.util.Runner;
import com.google.gson.Gson;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * Created by cloudera on 2/8/17.
 */
public class SparkVerticle extends AbstractVerticle{

    private Logger LOG = LoggerFactory.getLogger("SparkVerticle");

    private SparkSession spark;

    private Gson gson = new Gson();

    private Object createSparkSession() {
        SparkConf conf = new SparkConf().setMaster(config().getString("spark.master")).setAppName(config().getString("spark.appname"));

        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        return sparkSession;
    }


    public static void main(String argv[]){
        Runner.runExample(SparkVerticle.class);
    }

    public void start(){
        spark = (SparkSession) createSparkSession();

        /*EventBus eventBus = getVertx().eventBus();

        eventBus.registerDefaultCodec(CustomMessage.class, new CustomMessageCodec());

        eventBus.consumer(config().getString("eventbus.spark"), message -> {
            CustomMessage customMessage = (CustomMessage) message.body();

            System.out.println("Custom message received: "+customMessage.getSummary());
            handleEvent(customMessage);
            // Replying is same as publishing
            CustomMessage replyMessage = new CustomMessage(200, "a00000002", "Message sent from cluster receiver!");
            message.reply(replyMessage);
        });*/

        CopybookIngestionParameter param = CopybookIngestionParameter.apply("conn","RPWACT","/tmp/LRPWSACT.cpy","/tmp/RPWACT.FIXED.END","cp037","FixedLength","FMT_MAINFRAME","SplitNone");
        CustomMessage msg = new CustomMessage(1, "", gson.toJson(param));
        handleEvent(msg);
    }

    private void handleEvent(CustomMessage msg){

        switch (msg.getStatusCode())
        {
            case 1: {
                CopybookIngestionParameter a = gson.fromJson(msg.getSummary(), CopybookIngestionParameter.class);

                vertx.executeBlocking(future -> {
                    // Call some blocking API that takes a significant amount of time to return
                    Object result = CopybookIngestion.apply(config().getString("hdfs.conf"), spark.sqlContext(), a).run();
                    future.complete(result);
                }, res -> {
                    System.out.println("The result is: " + res.result());
                });
                break;
            }

            case 2: {
                UserClassParameter a = gson.fromJson(msg.getSummary(), UserClassParameter.class);

                vertx.executeBlocking(future -> {
                    // Call some blocking API that takes a significant amount of time to return
                    //Object result = RunUserClass.apply(jclFactory, jcl, sqlContext, a).run();
                    //future.complete(result);
                }, res -> {
                    System.out.println("The result is: " + res.result());
                });
                break;
            }

        }


    }
}
