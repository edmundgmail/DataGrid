package com.ddp;

import com.ddp.jarmanager.ScalaSourceCompiiler;
import com.ddp.jarmanager.ScalaSourceCompiiler$;
import com.ddp.utils.Utils;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.UpdateResult;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;

import java.io.BufferedInputStream;

/**
 * Created by eguo on 6/4/17.
 */
public class ScriptLoader {
    private JDBCClient client;
    private ScalaSourceCompiiler scalaSourceCompiiler;
    public ScriptLoader(JDBCClient client, ScalaSourceCompiiler scalaSourceCompiiler){
        this.client=client;
        this.scalaSourceCompiiler=scalaSourceCompiiler;
    }


    public void loadReports(String owner, String reportName, String hdfsFileNames) throws Exception{
        String names[] = hdfsFileNames.split(":");
        int i = 0;
        for(String filename : names){
            Path p = new Path(filename);
            String s = IOUtils.toString( new BufferedInputStream(Utils.getHdfs().open (p )), "UTF-8");
            loadReport(owner, reportName, p.getName(), s, i++);
        }

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
                                return;
                            }
                        });
            }
        });
    }

    public void load() {
        /*
        client.getConnection(res -> {
            if (res.succeeded()) {
                String sql = "select u1.owner, u1.name, u1.filename, u1.version, u1.content, u1.serial from userscript as u1 inner join (select owner, name, filename, max(version) as max_version from userscript group by owner, name, filename) u2 on u1.owner=u2.owner and u1.name=u2.name and u1.filename=u2.filename and u1.version=u2.max_version group by owner, name order by serial";
                res.result().query(sql, query -> {
                    if (query.succeeded()) {
                        String sources = "";
                        for (JsonObject s : query.result().getRows()) {
                            sources+=s.getString("content");
                        }
                        scalaSourceCompiiler.compile(sources);
                    }
                });
            }
        });*/
    }

}
