package com.ddp.util;

import com.ddp.access.UserParameter;
import com.google.gson.*;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by cloudera on 2/26/17.
 */
public class UserParameterDeserializer implements JsonDeserializer<UserParameter>
{
    static Map<String, Class<? extends UserParameter>> dataTypeRegistry = new HashMap<String, Class<? extends UserParameter>>();

    public void registerDataType(String clasName, Class<? extends UserParameter> javaType)
    {
        dataTypeRegistry.put(clasName, javaType);
    }

    @Override
    public UserParameter deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException
    {
        JsonObject jsonObject = json.getAsJsonObject();
        Class<? extends UserParameter> dataType = dataTypeRegistry.get(jsonObject.get("className").getAsString());
        if(dataType != null) {
            UserParameter ret = context.deserialize(jsonObject, dataType);
            //ret.className(jsonObject.get("className").getAsString()));
            return ret;
        }
        throw new RuntimeException("Oops");
    }
}
