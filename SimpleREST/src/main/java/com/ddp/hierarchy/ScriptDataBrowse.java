package com.ddp.hierarchy;

import com.ddp.access.NewDataSourceParameter;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.commons.lang.StringUtils;

import java.util.function.Consumer;

/**
 * Created by eguo on 6/7/17.
 */
public class ScriptDataBrowse implements IDataBrowse {
    private Logger LOGGER = LoggerFactory.getLogger(ScriptDataBrowse.class);
    private JDBCClient client;

    public ScriptDataBrowse(JDBCClient client){this.client=client;}


    public void handleUpdateHierarchy(Consumer<String> errHandler, Consumer<String> responseHandler, NewDataSourceParameter newDataSourceParameter) {
            //nothing to do here
    }

    private void listOwners(SQLConnection conn, Long id, int pageNum, int pageSize, Consumer<String> errorHandler, Consumer<String> responseHandler)
    {
        LOGGER.debug("in listOwners");
        conn.query("SELECT id, name, 'owner' as level from report_owners",  query -> {
            LOGGER.debug("in listOwners query returned");
            if (query.failed()) {
                LOGGER.debug("in listOwners query returned but failed");
                errorHandler.accept(query.cause().toString());
            } else {
                LOGGER.debug("in listOwners query returned and succeeded");

                if (query.result().getNumRows() == 0) {
                    errorHandler.accept("result has 0 row");
                } else {

                    responseHandler.accept(new JsonArray(query.result().getRows()).encode());
                }
            }
            conn.close();
        });
    }

    private void listReports(SQLConnection conn, Long id, int pageNum, int pageSize, Consumer<String> errorHandler, Consumer<String> responseHandler)
    {
        conn.queryWithParams("SELECT id, name, 'report' as level from reports where owner_id=?", new JsonArray().add(id) , query -> {
            if (query.failed()) {
                errorHandler.accept(query.cause().toString());
            } else {
                if (query.result().getNumRows() == 0) {
                    errorHandler.accept("result has 0 row");
                } else {

                    responseHandler.accept(new JsonArray(query.result().getRows()).encode());
                }
            }
            conn.close();
        });
    }

    private void listReportFiles(SQLConnection conn, Long id, int pageNum, int pageSize, Consumer<String> errorHandler, Consumer<String> responseHandler)
    {
        conn.queryWithParams("SELECT id, filename as name, 'file' as level, content from report_files where report_id=?", new JsonArray().add(id) , query -> {
            if (query.failed()) {
                errorHandler.accept(query.cause().toString());
            } else {
                if (query.result().getNumRows() == 0) {
                    errorHandler.accept("result has 0 row");
                } else {

                    responseHandler.accept(new JsonArray(query.result().getRows()).encode());
                }
            }
            conn.close();
        });
    }


    public void handleListHierarchy(Consumer<String> errorHandler, Consumer<String> responseHandler, int pageNum, int pageSize, String level, Long id){
        LOGGER.debug("in handleListHierarchy ");
        client.getConnection( res-> {
            LOGGER.debug("in handleListHierarchy got connection");
            if(res.succeeded()){
                LOGGER.debug("in handleListHierarchy  connection succeeded");
                if(StringUtils.isEmpty(level) || level.equalsIgnoreCase("root"))
                    listOwners(res.result(), id, pageNum, pageSize, errorHandler, responseHandler);
                else if(level.equalsIgnoreCase("owner"))
                    listReports(res.result(),id,  pageNum, pageSize, errorHandler, responseHandler);
                else if(level.equalsIgnoreCase( "report"))
                    listReportFiles(res.result(), id, pageNum, pageSize, errorHandler, responseHandler);
                else
                    errorHandler.accept("invalid parameter");
            }
        });
    }

    public void getEntityDetail(String entityName, Consumer<String> c){
    //nothing to do here
    }
}
