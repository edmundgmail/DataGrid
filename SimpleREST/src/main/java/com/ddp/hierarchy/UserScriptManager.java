package com.ddp.hierarchy;

import com.ddp.utils.Utils;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.jdbc.JDBCClient;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;

import java.io.BufferedInputStream;

/**
 * Created by cloudera on 6/6/17.
 */
public class UserScriptManager {
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

    public void loadUserScript(String level, String name, String parentLevel, String parentName){
        if(parentLevel.equals("root"))
        {

        }
        else if(parentLevel.equals("owner")) {

        }else if(parentLevel.equals("report")){

        }else if(parentLevel.equals("file"))
        {

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


}
