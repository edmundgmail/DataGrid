package com.ddp.hierarchy;

import com.ddp.access.IngestionParameter;
import com.ddp.access.NewDataSourceParameter;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import jdk.nashorn.api.scripting.JSObject;
import jodd.util.StringUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by cloudera on 1/24/17.
 */
public class DataBrowse implements IDataBrowse{

    private Logger LOGGER = LoggerFactory.getLogger(DataBrowse.class);
    private JDBCClient client;

    public DataBrowse(JDBCClient client){this.client=client;}

    public void handleUpdateHierarchy(Consumer<String> errHandler, Consumer<String> responseHandler, NewDataSourceParameter newDataSourceParameter){

        client.getConnection( res-> {
            if(res.succeeded()) {
                if(newDataSourceParameter.level().equalsIgnoreCase("datasource")){
                    String sql = "insert into datasource (sname, business_desc,nid) values (?, ?,0)";
                    res.result().updateWithParams(sql,
                            new JsonArray().add(newDataSourceParameter.name()).add(newDataSourceParameter.desc()),
                            (ar) -> {
                                if (ar.failed()) {
                                    errHandler.accept(ar.cause().toString());
                                    res.result().close();
                                    return;
                                }
                                UpdateResult result = ar.result();
                                // Build a new whisky instance with the generated id.
                                JsonObject o = new JsonObject();
                                o.put("id", result.getKeys().getLong(0));

                                responseHandler.accept(o.encode());
                                res.result().close();
                            });
                }
                else if(newDataSourceParameter.level().equalsIgnoreCase("dataentity")){
                    String sql = "insert into dataentity (sname, business_desc, nid, source_id) values (?, ?,0,?)";
                    res.result().updateWithParams(sql,
                            new JsonArray().add(newDataSourceParameter.name()).add(newDataSourceParameter.desc()).add(newDataSourceParameter.sourceId()),
                            (ar) -> {
                                if (ar.failed()) {
                                    errHandler.accept(ar.cause().toString());
                                    res.result().close();
                                    return;
                                }
                                UpdateResult result = ar.result();
                                // Build a new whisky instance with the generated id.
                                JsonObject o = new JsonObject();
                                o.put("id", result.getKeys().getLong(0));

                                responseHandler.accept(o.encode());
                                res.result().close();
                            });
                }

            }
    });
    }

    public void getEntityDetail(String entityName, Consumer<String> c) {
        if(StringUtils.isEmpty(entityName)){
            return;
        }

        String names[] = entityName.split("\\.");
        if(names.length!=2) {
            LOGGER.error("The entityName should be in sourceName.entityName format");
            return;
        }
        client.getConnection( res-> {
            if(res.succeeded()){
                res.result().queryWithParams("select f.sname from datafield f, dataentity e, datasource s where f.dataentity_id=e.dataentity_id and e.datasource_id=s.datasource_id and s.sname=? and e.sname=?", new JsonArray().add(names[0]).add(names[1]), query -> {
                    if(query.succeeded()){
                        c.accept(new JsonArray(query.result().getRows()).encode());
                    }
                    res.result().close();
                });
            }
        });

        /*try (SQLConnection conn = awaitResult(client::getConnection)) {
            ResultSet resultSet = awaitResult(h-> conn.queryWithParams(, h));
            return new JsonArray(resultSet.getRows()).encode();
        }catch (Exception e){
            LOGGER.error(e.getMessage());
            e.printStackTrace();
            return null;
        }*/
    }

    public void handleListHierarchy(Consumer<String> errorHandler, Consumer<String> responseHandler, int pageNum, int pageSize, String level, Long id) {
        client.getConnection( res-> {
            if(res.succeeded()){
                if(StringUtils.isEmpty(level))
                    listDataSources(res.result(), pageNum, pageSize, errorHandler, responseHandler);
                else if(level.equalsIgnoreCase("datasource"))
                    listDataEntities(res.result(),id,  pageNum, pageSize, errorHandler, responseHandler);
                else if(level.equalsIgnoreCase("dataentity"))
                    listDataFields(res.result(), id,  pageNum, pageSize, errorHandler, responseHandler);
                else
                    errorHandler.accept("invalid parameter");
            }
        });
    }


    private void listDataFields(SQLConnection conn, Long entityID,  int pageNum, int pageSize, Consumer<String> errorHandler, Consumer<String> responseHandler) {
        conn.queryWithParams("SELECT datafield_id as id, sname as name, 'datafield' as level, business_desc as description FROM datafield where entity_id=? LIMIT ?, ?", new JsonArray().add(entityID).add(pageNum).add(pageSize), query -> {
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

    private void listDataEntities(SQLConnection conn, Long sourceID,  int pageNum, int pageSize, Consumer<String> errorHandler, Consumer<String> responseHandler) {
        conn.queryWithParams("SELECT dataentity_id as id, sname as name, 'dataentity' as level, business_desc as description, source_id as sourceId FROM dataentity where source_id=? LIMIT ?, ?", new JsonArray().add(sourceID).add(pageNum).add(pageSize), query -> {
            if (query.failed()) {
                errorHandler.accept(query.cause().toString());
            } else {
                if (query.result().getNumRows() == 0) {
                    errorHandler.accept("result include 0 row");
                } else {

                    responseHandler.accept(new JsonArray(query.result().getRows()).encode());
                }
            }
            conn.close();
        });
    }

    private void listDataSources(SQLConnection conn,  int pageNum, int pageSize, Consumer<String> errorHandler, Consumer<String> responseHandler){
        conn.queryWithParams("SELECT datasource_id as id, sname as name, 'datasource' as level, business_desc as description FROM datasource LIMIT ?, ?", new JsonArray().add(pageNum).add(pageSize), query -> {
            if (query.failed()) {
                errorHandler.accept(query.cause().toString());
            } else {
                if (query.result().getNumRows() == 0) {
                    errorHandler.accept("result include no row");
                } else {

                    //String s = query.result().getRows().stream().map(r->r.encode()).reduce("", (a,b)->a+b);

                    responseHandler.accept(new JsonArray(query.result().getRows()).encode());
                }
            }
            conn.close();
        });
    }
}
