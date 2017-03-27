package nl.vu.datalayer.hbase.connection;

import nl.vu.datalayer.hbase.Quorum;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.PoolMap;

import java.io.IOException;

public class NativeJavaConnection extends HBaseConnection {
	
	protected HBaseAdmin hbase = null;
	protected Configuration conf = null;
	private MyTablePool tablePool = null;
	
	public static final int MAX_POOL_SIZE = 20;
	
	public NativeJavaConnection() throws IOException {
		//add the "hbase-site.xml" file to the classpath to get the bootstrapping Zookeeper nodes
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", Quorum.quorums);
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		tablePool = new MyTablePool(conf, MAX_POOL_SIZE, PoolMap.PoolType.Reusable);
		hbase = new HBaseAdmin(conf);
	}
	
	public void initTables(String []tableNames) throws IOException{
		int iterations = MAX_POOL_SIZE/tableNames.length;
		HTable [][]tables = new HTable[iterations][];
		
		//open tables
		for (int i = 0; i < iterations; i++) {
			tables[i] = new HTable[tableNames.length];
			for (int j = 0; j < tableNames.length; j++) {
				if (hbase.tableExists(tableNames[j])){
					tables[i][j] = (HTable) (((MyTablePool.PooledHTable)
							tablePool.getTable(tableNames[j])).getWrappedTable());
				//	tables[i][j].setRegionCachePrefetch(tables[i][j].getName(),true);
				}
				else{ 
					tables[i][j] = null;
				}
			}
		}
		
		//close tables
		for (int i = 0; i < tables.length; i++) {
			for (int j = 0; j < tableNames.length; j++) {
				if (tables[i][j]!=null){
					tables[i][j].close();
				}
			}
		}
	}

	public Table getTable(String tableName) throws IOException{
		return ((MyTablePool.PooledHTable)tablePool.getTable(tableName)).getWrappedTable();
	}

	public Table getTable(byte [] tableNameBytes) throws IOException{
		return tablePool.getTable(tableNameBytes);
	}

	public void close() throws IOException{
		hbase.close();
		tablePool.close();
	}
	
	public HBaseAdmin getAdmin() {
		return hbase;
	}
	
	public Configuration getConfiguration() {
		return conf;
	}

}
