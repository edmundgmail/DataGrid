package com.ddp;

/**
 * Created by eguo on 4/4/17.
 */
import com.ddp.domain.JobDefinition;
import com.ddp.domain.JobDescriptor;
import com.google.common.base.*;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;

import java.util.*;
import java.util.Optional;

public class SchedVerticle extends AbstractVerticle {
    private static final Scheduler scheduler = scheduler();
    private static final List<JobDefinition> definitions = new ArrayList<JobDefinition>();

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
        }
        catch(SchedulerException e){
            e.printStackTrace();
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
            jobDescriptor.setGroup(group);
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
