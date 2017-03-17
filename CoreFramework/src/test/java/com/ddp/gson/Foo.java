package com.ddp.gson;

/**
 * Created by cloudera on 2/26/17.
 */
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

public class Foo
{
    public static void main(String[] args)
    {
    /*
      {
        "response": {"data": {"foo": "FOO"}},
        "meta": {"errors": [], "success": 1}
      }
     */
        String input1 = "{\"response\": {\"data\": {\"foo\": \"FOO\"}},\"meta\": {\"errors\": [], \"success\": 1}}";

    /*
      {
        "response": {"data": {"bar": 42}},
        "meta": {"errors": [], "success": 1}
      }
     */
        String input2 = "{\"response\": {\"data\": {\"bar\": 42}},\"meta\": {\"errors\": [], \"success\": 1}}";

        processInput(input1);
        // {"response":{"data":{"foo":"FOO"}},"meta":{"errors":[],"success":1}}

        processInput(input2);
        // {"response":{"data":{"bar":42}},"meta":{"errors":[],"success":1}}
    }

    static void processInput(String jsonInput)
    {
        DataDeserializer dataDeserializer = new DataDeserializer();
        dataDeserializer.registerDataType("foo", A.class);
        dataDeserializer.registerDataType("bar", B.class);

        Gson gson = new GsonBuilder().registerTypeAdapter(Data.class, dataDeserializer).create();

        BaseResponse response = gson.fromJson(jsonInput, BaseResponse.class);
        System.out.println(new Gson().toJson(response));
        System.out.println("classname=" + response.response.data.getClass().getName());
    }
}

class DataDeserializer implements JsonDeserializer<Data>
{
    Map<String, Class<? extends Data>> dataTypeRegistry = new HashMap<String, Class<? extends Data>>();

    void registerDataType(String jsonElementName, Class<? extends Data> javaType)
    {
        dataTypeRegistry.put(jsonElementName, javaType);
    }

    @Override
    public Data deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException
    {
        JsonObject jsonObject = json.getAsJsonObject();
        for (String elementName : dataTypeRegistry.keySet())
        {
            if (jsonObject.has(elementName))
            {
                Class<? extends Data> dataType = dataTypeRegistry.get(elementName);
                return context.deserialize(jsonObject, dataType);
            }
        }
        throw new RuntimeException("Oops");
    }
}

class BaseResponse
{
    Response response;
    Meta meta;
}

class Meta
{
    List<String> errors;
    int success;
}

class Response
{
    Data data;
}

class Data
{

}

class A extends Data
{
    String foo;
}

class B extends Data
{
    int bar;
}