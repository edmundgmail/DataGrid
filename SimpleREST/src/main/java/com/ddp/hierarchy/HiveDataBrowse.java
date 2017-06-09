package com.ddp.hierarchy;

import com.ddp.access.NewDataSourceParameter;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.util.List;
import java.util.function.Consumer;

/**
 * Created by cloudera on 6/1/17.
 */
public class HiveDataBrowse  implements IDataBrowse {
    private Logger LOGGER = LoggerFactory.getLogger(HiveDataBrowse.class);
    private HiveMetaStoreClient hiveMetaStoreClient;

    public HiveDataBrowse(){
        try{
            HiveConf c=new HiveConf();
            hiveMetaStoreClient=new HiveMetaStoreClient(c);
        }
        catch (MetaException e){
            LOGGER.error("Exception ", e);
        }
    }

    private void listDatabases(){
        try{
            List<String> s = hiveMetaStoreClient.getAllDatabases();
            s.forEach(d->System.out.println(d));
        }
        catch (MetaException e){
            LOGGER.error("Exception ", e);
        }

    }

    public static void main(String args[]){
        HiveDataBrowse hiveDataBrowse = new HiveDataBrowse();
        hiveDataBrowse.listDatabases();
    }

    public void handleUpdateHierarchy(Consumer<String> errHandler, Consumer<String> responseHandler, NewDataSourceParameter newDataSourceParameter) {

    }

    public void handleListHierarchy(Consumer<String> errHandler, Consumer<String> responseHandler, int pageNum, int pageSize, String level, Long id){
    }

    public void getEntityDetail(String entityName, Consumer<String> c){

    }
}

