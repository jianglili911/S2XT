package org.buaa.nlsde.jianglili.query.hbaserdf.utils;

import nl.vu.datalayer.hbase.Quorum;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * Created by jianglili on 2017/2/3.
 */
public class loadLubmTest {




    public static void main(String[] args) {
       loadBulk(args);
      //  loadTriple(args);
    }

    public static void loadBulk(String[] args){
        args=("E:\\benchmarks\\LUBM\\datasets\\nt1-1\\University_1.nt " +
                "file:///E:\\benchmarks\\LUBM\\datasets\\hbasebulk\\lubmtest").split(" ");
        Quorum.confFile="config"+"_test"+".properties";
        nl.vu.datalayer.hbase.bulkload.BulkLoad.main(args);
    }
    public static void loadTriple(String[] args){

        loadLib();
        //hexastore   predicate-cf  prefix-match
        args=("E:\\benchmarks\\LUBM\\datasets\\nt1-1\\University_1.nt " +
                "prefix-match").split(" ");
        Quorum.confFile="config"+"_test"+".properties";
        nl.vu.datalayer.hbase.NTripleParser.main(args);
    }

    public static void loadLib(){
        String osName=System.getProperty("os.name");
        if(osName.toLowerCase().startsWith("windows")) {
            System.loadLibrary("hadoop");
        }

    }
}
