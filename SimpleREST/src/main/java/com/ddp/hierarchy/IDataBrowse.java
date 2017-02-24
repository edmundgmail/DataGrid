package com.ddp.hierarchy;

import java.util.function.Consumer;

/**
 * Created by cloudera on 1/24/17.
 */
public interface IDataBrowse {
    public void handleListHierarchy(Consumer<Integer> errHandler, Consumer<String> responseHandler, int pageNum, int pageSize, Long sourceID, Long entityID);
}
