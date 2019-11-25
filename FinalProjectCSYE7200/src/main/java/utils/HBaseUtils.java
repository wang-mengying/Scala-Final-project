package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;

public class HBaseUtils {
    HBaseAdmin admin = null;
    Configuration configuration = null;

    private HBaseUtils(){
        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum","hadoop000:2181");
        configuration.set("hbase.rootdir","hdfs://hadoop000:8020/hbase");
        try {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HBaseUtils instance = null;

    public static synchronized HBaseUtils getInstance(){
        if(instance == null) instance = new HBaseUtils();
        return instance;
    }

    public HTable getTable(String tableName){
        HTable table = null;
        try {
            table = new HTable(configuration,tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

}