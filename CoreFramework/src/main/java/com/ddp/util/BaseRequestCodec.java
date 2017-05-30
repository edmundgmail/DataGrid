package com.ddp.util;

import com.ddp.access.BaseRequest;
import com.ddp.access.UserParameter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;

/**
 * Vert.x core example for {@link io.vertx.core.eventbus.EventBus} and {@link MessageCodec}
 * @author Junbong
 */
public class BaseRequestCodec implements MessageCodec<BaseRequest, BaseRequest> {
  private UserParameterDeserializer userParameterDeserializer = UserParameterDeserializer.getInstance();
  private Gson gsonDe = new GsonBuilder().registerTypeAdapter(UserParameter.class, userParameterDeserializer).create();

  private Gson gsonSer = new Gson();
  @Override
  public void encodeToWire(Buffer buffer, BaseRequest customMessage) {
    // Encode object to string
    String jsonToStr = gsonSer.toJson(customMessage);

    // Length of JSON: is NOT characters count
    int length = jsonToStr.getBytes().length;

    // Write data into given buffer
    buffer.appendInt(length);
    buffer.appendString(jsonToStr);
  }

  @Override
  public BaseRequest decodeFromWire(int position, Buffer buffer) {
    // My custom message starting from this *position* of buffer
    int _pos = position;

    // Length of JSON
    int length = buffer.getInt(_pos);

    // Get JSON string by it`s length
    // Jump 4 because getInt() == 4 bytes
    String jsonStr = buffer.getString(_pos+=4, _pos+=length);

    // We can finally create custom message object
    return gsonDe.fromJson(jsonStr, BaseRequest.class);
  }

  @Override
  public BaseRequest transform(BaseRequest customMessage) {
    // If a message is sent *locally* across the event bus.
    // This example sends message just as is
    return customMessage;
  }

  @Override
  public String name() {
    // Each codec must have a unique name.
    // This is used to identify a codec when sending a message and for unregistering codecs.
    return this.getClass().getSimpleName();
  }

  @Override
  public byte systemCodecID() {
    // Always -1
    return -1;
  }
}
