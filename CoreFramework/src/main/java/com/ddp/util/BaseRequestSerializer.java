package com.ddp.util;

import com.ddp.access.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Created by cloudera on 2/27/17.
 */
public class BaseRequestSerializer implements Serializer<BaseRequest> {

    private Gson gson;
    private UserParameterDeserializer userParameterDeserializer;

    public void configure(Map<String, ?> configs, boolean isKey) {

        gson = new Gson();
    }

    public byte[] serialize(String topic, BaseRequest data) {
        try {
            return gson.toJson(data).getBytes();
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to BaseRequest due to unsupported encoding ");
        }
    }

    public void close() {
    }
}
