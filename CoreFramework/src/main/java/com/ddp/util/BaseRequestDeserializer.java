package com.ddp.util;

import com.ddp.access.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * Created by cloudera on 2/27/17.
 */
public class BaseRequestDeserializer implements Deserializer<BaseRequest> {

    private Gson gson;
    private UserParameterDeserializer userParameterDeserializer;

    public void configure(Map<String, ?> configs, boolean isKey) {

        userParameterDeserializer = new UserParameterDeserializer();

        userParameterDeserializer.registerDataType(CopybookIngestionParameter.class.getName(), CopybookIngestionParameter.class);
        userParameterDeserializer.registerDataType(JarParamter.class.getName(), JarParamter.class);
        userParameterDeserializer.registerDataType(ScalaSourceParameter.class.getName(), ScalaSourceParameter.class);

        gson = new GsonBuilder().registerTypeAdapter(UserParameter.class, userParameterDeserializer).create();


    }

    public BaseRequest deserialize(String topic, byte[] data) {
        try {
            return gson.fromJson(new String(data), BaseRequest.class);
        } catch (Exception e) {
            //throw new SerializationException("Error when deserializing byte[] to BaseRequest due to unsupported encoding ");
            return null;
        }
    }

    public void close() {
    }
}
