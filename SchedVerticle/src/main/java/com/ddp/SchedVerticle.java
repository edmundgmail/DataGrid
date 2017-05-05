package com.ddp;

/**
 * Created by eguo on 4/4/17.
 */
import com.ddp.access.BaseRequest;
import com.ddp.domain.HttpJobDefinition;
import com.ddp.domain.JobDefinition;
import com.ddp.domain.JobDescriptor;
import com.ddp.util.CustomMessage;
import com.ddp.util.CustomMessageCodec;
import com.google.common.base.*;
import com.google.gson.Gson;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.CorsHandler;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;

import java.util.*;
import java.util.function.Consumer;

public class SchedVerticle extends AbstractVerticle {
    private Logger LOGGER = LoggerFactory.getLogger("SchedVerticle");

    private static final Scheduler scheduler = scheduler();
    private static final List<JobDefinition> definitions = new ArrayList<JobDefinition>();
    private static String toScheduleEvent;
    private static Gson gson = new Gson();
    private static Integer httpPort;
    private static void initConfig(JsonObject js) {
        toScheduleEvent = js.getString("eventbus.schedverticle");
        httpPort = js.getInteger("job.httpport");
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
                        vertx.deployVerticle(SchedVerticle.class.getName(), deploymentOptions);
                    } else {
                        System.err.println("Oh oh ..." + result.cause());
                    }
                });

            }
        });
    }

    private static Scheduler scheduler() {
        try {
            return StdSchedulerFactory.getDefaultScheduler();
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start() {
        try{
            if(scheduler != null)
                scheduler.start();
        }catch(SchedulerException e){
            e.printStackTrace();
        }

        Router router = Router.router(vertx);

        router.route().handler(
                CorsHandler.create("*")
                        .allowedMethod(HttpMethod.GET)
                        .allowedMethod(HttpMethod.POST)
                        .allowedMethod(HttpMethod.OPTIONS)
                        .allowedHeader("X-PINGARUNER")
                        .allowedHeader("Content-Type")
        );

        router.get("/jobs").handler(this::getJobs);
        router.post("/groups/jobs").handler(this::postJobs);

        vertx.createHttpServer().requestHandler(router::accept).listen(httpPort);
    }

    private void postJobs(RoutingContext routingContext)
    {
        HttpServerResponse response = routingContext.response();
        Consumer<Integer> errorHandler = i -> response.setStatusCode(i).end();
        Consumer<String> responseHandler = s -> response.putHeader("content-type", "application/json").end(s);

        routingContext.request().bodyHandler(new Handler<Buffer>() {
            @Override
            public void handle(Buffer buffer) {
                LOGGER.info("buffer=" + buffer.toString());
                JobDescriptor jobDescriptor = gson.fromJson(buffer.toString(), HttpJobDefinition.HttpJobDescriptor.class);

                JobDescriptor ret = addJob("", jobDescriptor);
                String messageBody = gson.toJson(ret);
                responseHandler.accept(messageBody);
            }
        });

    }

    private void getJobs(RoutingContext routingContext){
        HttpServerResponse response = routingContext.response();
        Consumer<Integer> errorHandler = i-> response.setStatusCode(i).end();
        Consumer<String> responseHandler = s-> response.putHeader("content-type", "application/json").end(s);

        try{
            //String messgaeBody = gson.toJson(scheduler.getJobKeys(GroupMatcher.anyJobGroup()));
            for (String groupName : scheduler.getJobGroupNames()) {

                for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {

                    String jobName = jobKey.getName();
                    String jobGroup = jobKey.getGroup();

                    List<Trigger> triggers = (List<Trigger>) scheduler.getTriggersOfJob(jobKey);
                    Date nextFireTime = triggers.get(0).getNextFireTime();

                    System.out.println("[jobName] : " + jobName + " [groupName] : "
                            + jobGroup + " - " + nextFireTime);
                }
            }

            //responseHandler.accept(messgaeBody);
        }catch (SchedulerException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop(){
        try {
            if(scheduler != null){
                scheduler.shutdown();
            }

        }catch(SchedulerException e){
            e.printStackTrace();
        }
    }

    public JobDescriptor addJob(String group, JobDescriptor jobDescriptor) {
        try {
            //jobDescriptor.setGroup(group);
            Set<Trigger> triggers = jobDescriptor.buildTriggers();
            JobDetail jobDetail = jobDescriptor.buildJobDetail();
            if (triggers.isEmpty()) {
                scheduler.addJob(jobDetail, false);
            } else {
                scheduler.scheduleJob(jobDetail, triggers, false);
            }
            return jobDescriptor;
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<JobKey> getJobKeys()  {
        try {
            return scheduler.getJobKeys(GroupMatcher.anyJobGroup());
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<JobKey> getJobKeysByGroup(String group)  {
        try {
            return scheduler.getJobKeys(GroupMatcher.jobGroupEquals(group));
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    public com.google.common.base.Optional<JobDescriptor> getJob(String group, String name) {
        try {
            JobDetail jobDetail = scheduler.getJobDetail(new JobKey(name, group));

            if (jobDetail == null) {
                return com.google.common.base.Optional.absent();
            }

            for (JobDefinition definition : definitions) {
                if (definition.acceptJobClass(jobDetail.getJobClass())) {
                    return com.google.common.base.Optional.of(definition.buildDescriptor(
                            jobDetail, scheduler.getTriggersOfJob(jobDetail.getKey())));
                }
            }

            throw new IllegalStateException("can't find job definition for " + jobDetail
                    + " - available job definitions: " + definitions);
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteJob(String group, String name) {
        try {
            scheduler.deleteJob(new JobKey(name, group));
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }
}
