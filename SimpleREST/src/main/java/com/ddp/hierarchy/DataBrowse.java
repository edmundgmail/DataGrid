package com.ddp.hierarchy;

import com.ddp.access.IngestionParameter;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import jodd.util.StringUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.function.Consumer;
import java.util.function.Function;
import static io.vertx.ext.sync.Sync.awaitResult;
import co.paralleluniverse.fibers.Suspendable;

/**
 * Created by cloudera on 1/24/17.
 */
public class DataBrowse implements IDataBrowse{

    private Logger LOGGER = LoggerFactory.getLogger(DataBrowse.class);
    private JDBCClient client;

    public DataBrowse(JDBCClient client){this.client=client;}

    @Suspendable
    public String getEntityDetail(String entityName) {
        if(StringUtils.isEmpty(entityName)){
            return null;
        }

        String names[] = entityName.split("\\.");
        if(names.length!=2) {
            LOGGER.error("The entityName should be in sourceName.entityName format");
            return null;
        }

        try (SQLConnection conn = awaitResult(client::getConnection)) {
            ResultSet resultSet = awaitResult(h-> conn.queryWithParams("select f.sname from datafield f, dataentity e, datasource s where f.entity_id=e.entity_id and e.source_id=s.source_id and s.sname=? and e.sname=?", new JsonArray().add(names[0]).add(names[1]), h));
            return new JsonArray(resultSet.getRows()).encode();
        }catch (Exception e){
            LOGGER.error(e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

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
