package com.ddp.hierarchy;

import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;

import java.util.function.Consumer;

/**
 * Created by cloudera on 1/24/17.
 */
public class DataBrowse implements IDataBrowse{

    private Logger LOGGER = LoggerFactory.getLogger(DataBrowse.class);
    private JDBCClient client;

    public DataBrowse(JDBCClient client){this.client=client;}

    public void handleListHierarchy(Consumer<Integer> errorHandler, Consumer<String> responseHandler, int pageNum, int pageSize, Long sourceID, Long entityID) {
        client.getConnection( res-> {
            if(res.succeeded()){

                if(sourceID==0)
                    listDataSources(res.result(), pageNum, pageSize, errorHandler, responseHandler);
                else if(entityID ==0)
                    listDataEntities(res.result(),sourceID,  pageNum, pageSize, errorHandler, responseHandler);
                else
                    listDataFields(res.result(), entityID,  pageNum, pageSize, errorHandler, responseHandler);
            }
        });
    }


    private void listDataFields(SQLConnection conn, Long entityID,  int pageNum, int pageSize, Consumer<Integer> errorHandler, Consumer<String> responseHandler) {
        conn.queryWithParams("SELECT sname FROM datafield where entity_id=? LIMIT ?, ?", new JsonArray().add(entityID).add(pageNum).add(pageSize), query -> {
            if (query.failed()) {
                errorHandler.accept(500);
            } else {
                if (query.result().getNumRows() == 0) {
                    errorHandler.accept(403);
                } else {

                    responseHandler.accept(new JsonArray(query.result().getRows()).encode());
                }
            }
        });
    }

    private void listDataEntities(SQLConnection conn, Long sourceID,  int pageNum, int pageSize, Consumer<Integer> errorHandler, Consumer<String> responseHandler) {
        conn.queryWithParams("SELECT sname FROM dataentity where source_id=? LIMIT ?, ?", new JsonArray().add(sourceID).add(pageNum).add(pageSize), query -> {
            if (query.failed()) {
                errorHandler.accept(500);
            } else {
                if (query.result().getNumRows() == 0) {
                    errorHandler.accept(403);
                } else {

                    responseHandler.accept(new JsonArray(query.result().getRows()).encode());
                }
            }
        });
    }

    private void listDataSources(SQLConnection conn,  int pageNum, int pageSize, Consumer<Integer> errorHandler, Consumer<String> responseHandler){
        conn.queryWithParams("SELECT sname FROM datasource LIMIT ?, ?", new JsonArray().add(pageNum).add(pageSize), query -> {
            if (query.failed()) {
                errorHandler.accept(500);
            } else {
                if (query.result().getNumRows() == 0) {
                    errorHandler.accept(403);
                } else {

                    //String s = query.result().getRows().stream().map(r->r.encode()).reduce("", (a,b)->a+b);

                    responseHandler.accept(new JsonArray(query.result().getRows()).encode());
                }
            }
        });
    }
}
