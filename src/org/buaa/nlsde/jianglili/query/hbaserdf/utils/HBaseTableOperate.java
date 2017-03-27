package org.buaa.nlsde.jianglili.query.hbaserdf.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * Created by jianglili on 2017/1/3.
 */
public class HBaseTableOperate {

    public static HBaseAdmin getAdmin() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.2.28.66");//使用eclipse时必须添加这个，否则无法定位
     //   conf.set("hbase.zookeeper.quorum", "10.2.1.141");//使用eclipse时必须添加这个，否则无法定位
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        HBaseAdmin admin = new HBaseAdmin(conf);// 新建一个数据库管理员
        return admin;
    }

    public static void dropTables() throws IOException {
        HBaseAdmin admin=getAdmin();
//        String[] tables={"Counterslubm", "Id2Stringlubm", "OSPClubm",
//                "POCSlubm", "SPOClubm", "String2Idlubm"};
   //     String[] tables={"O-SP_lubm1", "P-SO_lubm1", "PO-S_lubm1", "S-PO_lubm1", "SO-P_lubm1", "SP-O_lubm1", "SPO_lubm1"};
//        String[] tables={"CPSO_lubmtest", "CSPO_lubmtest", "Counters_lubmtest","Id2String_lubmtest",
//                 "OCSP_lubmtest", "OSPC_lubmtest", "POCS_lubmtest","SPOC_lubmtest", "String2Id_lubmtest"};

//        String[] tables={"O-SP_lubm1",  "P-SO_lubm1", "PO-S_lubm1",  "S-PO_lubm1",
//                "SO-P_lubm1", "SP-O_lubm1", "SPO_lubm1"};


        String[] tables={"Counters_lubm1","Id2String_lubm1",
               "OSPC_lubm1", "POCS_lubm1",
                  "SPOC_lubm1", "String2Id_lubm1",};
        for (String table:tables)
            if(admin.tableExists(table)) {
                admin.disableTable(table);
                admin.deleteTable(table);
                System.out.println("dtop table:"+table);
            }
    }




    public static void scanTables() throws IOException {
        HBaseAdmin admin=getAdmin();
        Configuration conf=admin.getConfiguration();
        String[] tables={"Counters_lubm", "Id2String_lubm", "OSPC_lubm",
                "POCS_lubm", "SPOC_lubm", "String2Id_lubm"};
        for (String tableName:tables)
            if(admin.tableExists(tableName)) {
                System.out.println(tableName+": exists!");
                HTable table = new HTable(conf, tableName);
                Scan scan = new Scan();
                ResultScanner results = table.getScanner(scan);
                // 输出结果
                for (Result result : results) {
                    for (KeyValue rowKV : result.raw()) {
                        System.out.print("Row Name: " + new String(rowKV.getRow()) + " ");
                        System.out.print("Timestamp: " + rowKV.getTimestamp() + " ");
                        System.out.print("column Family: " + new String(rowKV.getFamily()) + " ");
                        System.out.print("Row Name:  " + new String(rowKV.getQualifier()) + " ");
                        System.out.println("Value: " + new String(rowKV.getValue()) + " ");
                    }
                }
            }
    }
    public static void countTables() throws Exception {
        HBaseAdmin admin=getAdmin();
        Configuration conf=admin.getConfiguration();
        String[] tables={"Counters_lubm", "Id2String_lubm", "OSPC_lubm",
                "POCS_lubm", "SPOC_lubm", "String2Id_lubm"};
        for (String tableName:tables)
            if(admin.tableExists(tableName)) {
                System.out.println(tableName+": exists!");
                String coprocessorClassName = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
                HBaseUtils.addTableCoprocessor(admin,tableName, coprocessorClassName);
                long rowCount = HBaseUtils.rowCount(tableName);
                System.out.println("rowCount: " + rowCount);
            }
    }
    public static void main(String[] args) throws Exception {
//       long lon= HBHexastoreOperationManager
//               .hashFunction("<http://xmlns.com/foaf/0.1/name>");
//        byte[] hashes=Bytes.toBytes(lon);
//        System.out.println(hashes);
        dropTables();



    }
}

