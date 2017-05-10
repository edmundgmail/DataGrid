package com.ddp.hierarchy;

import com.ddp.access.IngestionParameter;
import io.vertx.core.json.JsonArray;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by cloudera on 1/24/17.
 */
public interface IDataBrowse {
    public void handleListHierarchy(Consumer<Integer> errHandler, Consumer<String> responseHandler, int pageNum, int pageSize, String level, Long id);
    public void getEntityDetail(String entityName, Consumer<String> c);
}
