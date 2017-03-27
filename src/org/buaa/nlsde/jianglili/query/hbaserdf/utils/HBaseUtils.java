package org.buaa.nlsde.jianglili.query.hbaserdf.utils;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;


/**
 * Created by jianglili on 2017/2/1.
 */
public class HBaseUtils {
    public static Logger logger= Logger.getLogger(HBaseUtils.class);

    private static HBaseConfiguration configuration;
    public static void addTableCoprocessor(HBaseAdmin admin, String tableName, String coprocessorClassName) {
        try {
            admin.disableTable(tableName);
            HTableDescriptor htd = admin.getTableDescriptor(Bytes.toBytes(tableName));
            htd.addCoprocessor(coprocessorClassName);
            admin.modifyTable(Bytes.toBytes(tableName), htd);
            admin.enableTable(tableName);
            configuration=(HBaseConfiguration) admin.getConfiguration();
        } catch (IOException e) {
            logger.info(e.getMessage(), e);
        }
    }

    public static long rowCount(String tableName) {
        AggregationClient ac = new AggregationClient(configuration);
        Scan scan = new Scan();
       // scan.addFamily(Bytes.toBytes(family));
        long rowCount = 0;
        try {

            HTable table = new HTable(configuration, tableName);
            rowCount = ac.rowCount(table, new LongColumnInterpreter(), scan);
        } catch (Throwable e) {
            logger.info(e.getMessage(), e);
        }
        return rowCount;
    }
}
