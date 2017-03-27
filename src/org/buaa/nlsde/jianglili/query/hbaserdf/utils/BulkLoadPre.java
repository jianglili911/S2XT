package org.buaa.nlsde.jianglili.query.hbaserdf.utils;

import io.netty.channel.EventLoopGroup;
import nl.vu.datalayer.hbase.Quorum;

/**
 * Created by jianglili on 2017/2/4.
 */
public class BulkLoadPre{

    public static void main(String[] args) {
        Quorum.confFile="config"+args[0]+".properties";
        String[] args2=new String[args.length-1];
        for(int i=0;i<args.length-1;i++)
           args2[i]=args[i+1];
        nl.vu.datalayer.hbase.bulkload.BulkLoad.main(args2);
        EventLoopGroup eventExecutors;
    }
}
