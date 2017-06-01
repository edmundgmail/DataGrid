package com.ddp.util;

import com.ddp.access.*;
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
    private UserParameterDeserializer(){

    }

    public static UserParameterDeserializer getInstance(){
        UserParameterDeserializer instance = new UserParameterDeserializer();
        instance.registerDataType(CopybookIngestionParameter.class.getName(), CopybookIngestionParameter.class);
        instance.registerDataType(JarParamter.class.getName(), JarParamter.class);
        instance.registerDataType(ScalaSourceParameter.class.getName(), ScalaSourceParameter.class);
        instance.registerDataType(QueryParameter.class.getName(), QueryParameter.class);
        instance.registerDataType(UserClassParameter.class.getName(), UserClassParameter.class);
        instance.registerDataType(CsvIngestionParameter.class.getName(), CsvIngestionParameter.class);
        instance.registerDataType(xmlIngestionParameter.class.getName(), xmlIngestionParameter.class);
        instance.registerDataType(NewDataSourceParameter.class.getName(), NewDataSourceParameter.class);
        instance.registerDataType(SparkResponseParameter.class.getName(), SparkResponseParameter.class);
        instance.registerDataType(HiveHierarchyParameter.class.getName(), HiveHierarchyParameter.class);
        return instance;
    }

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
