package com.ddp.hierarchy;

import com.ddp.utils.Utils;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;

import java.io.BufferedInputStream;
import java.util.function.Consumer;

/**
 * Created by cloudera on 6/6/17.
 */
public class UserScriptManager {

    private Logger LOGGER = LoggerFactory.getLogger("UserScriptManager");

    private JDBCClient client;

    public UserScriptManager(JDBCClient client){this.client=client;}



    public void loadReports(String owner, String reportName, String hdfsFileNames) throws Exception{
        String names[] = hdfsFileNames.split(":");
        int i = 0;
        for(String filename : names){
            Path p = new Path(filename);
            String s = IOUtils.toString( new BufferedInputStream(Utils.getHdfs().open (p )), "UTF-8");
            loadReport(owner, reportName, p.getName(), s, i++);
        }

    }

    public void removeUserScript(Consumer<String> responseHandler,Consumer<String> errorHandler, String level, Integer id)
    {
        client.getConnection(res -> {
            String sql = "";
            if (res.succeeded()) {
                if(level.equalsIgnoreCase("owner"))
                {
                    sql = "delete from report_owners where id=?";
                }
                else if(level.equalsIgnoreCase("report")){
                    sql = "delete from reports where id=?";
                }else if (level.equalsIgnoreCase("file")){
                    sql = "delete from report_files where id=?";
                }
                else{
                    errorHandler.accept("invalid deletion parameter");
                }

                res.result().updateWithParams(sql,
                        new JsonArray().add(id),
                        (ar) -> {
                            if (ar.failed()) {
                                errorHandler.accept(ar.cause().toString());
                            }
                            else{
                                responseHandler.accept(ar.result().toJson().encode());
                            }
                        });
            }
            res.result().close();
        });


    }

    public void loadUserScript(Consumer<String> responseHandler, Consumer<String> errorHandler, String level, String name, String content, Integer id, Integer parentId){

        client.getConnection(res -> {
            String sql;
            if(level.equals("owner")) {
                if(id > 0)
                {
                    sql = "update report_owners set name=? where id=?";
                    res.result().updateWithParams(sql,
                            new JsonArray().add(name).add(id),
                            (ar) -> {
                                if (ar.failed()) {
                                    errorHandler.accept(ar.cause().toString());
                                }else {
                                    responseHandler.accept(ar.result().toJson().encode());
                                }
                            });
                }
                else{
                    sql = "insert into  report_owners(name) values ( ? ) ";
                    res.result().updateWithParams(sql,
                            new JsonArray().add(name),
                            (ar) -> {
                                if (ar.failed()) {
                                    errorHandler.accept(ar.cause().toString());
                                }
                                else
                                {
                                    responseHandler.accept(ar.result().toJson().encode());
                                }
                            });
                }

            }else if(level.equals("report")){
                if(id > 0)
                {
                    sql = "update reports set name=? where id=? ";
                    res.result().updateWithParams(sql,
                            new JsonArray().add(name).add(id),
                            (ar) -> {
                                if (ar.failed()) {
                                    errorHandler.accept(ar.cause().toString());
                                }
                                else{
                                    responseHandler.accept(ar.result().toJson().encode());
                                }

                            });
                }
                else {
                    sql = "insert into  reports (owner_id, name) values (?, ?) ";
                    res.result().updateWithParams(sql,
                            new JsonArray().add(parentId).add(name),
                            (ar) -> {
                                if (ar.failed()) {
                                    errorHandler.accept(ar.cause().toString());
                                }
                                else{
                                    responseHandler.accept(ar.result().toJson().encode());
                                }
                            });
                }
            }else if(level.equals("file"))
            {
                if(id > 0)
                {
                    sql = "update report_files set filename=?, content=? where id=?";
                    res.result().updateWithParams(sql,
                            new JsonArray().add(name).add(content).add(id),
                            (ar) -> {
                                if (ar.failed()) {
                                    errorHandler.accept(ar.cause().toString());
                                }
                                else{
                                    responseHandler.accept(ar.result().toJson().encode());
                                }
                            });

                }
                else
                {
                    sql = "insert into  report_files (report_id, filename, content) values (?, ?, ?) ";
                    res.result().updateWithParams(sql,
                            new JsonArray().add(parentId).add(name).add(content),
                            (ar) -> {
                                if (ar.failed()) {
                                    errorHandler.accept(ar.cause().toString());
                                }
                                else
                                {
                                    responseHandler.accept(ar.result().toJson().encode());
                                }
                            });
                }
            }
            res.result().close();
        });

    }


    public void loadReport(String owner, String reportName, String fileName, String content, int serial)
    {
        client.getConnection(res -> {
            if (res.succeeded()) {
                String sql = "insert into userscript (owner, name, version, filename, content, cdate, serial) select ?,?, max(version)+1 ,?,?,now(),? from userscript where owner= ? and name=? and filename=?";
                res.result().updateWithParams(sql,
                        new JsonArray().add(owner).add(reportName).add(fileName).add(content).add(serial).add(owner).add(reportName).add(fileName),
                        (ar) -> {
                            if (ar.failed()) {
                                res.result().close();
                                return;
                            }
                        });
                res.result().close();
            }
        });
    }


}
