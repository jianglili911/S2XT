package nl.vu.datalayer.hbase.coprocessor;

import nl.vu.datalayer.hbase.bulkload.ResourceToTriple;
import nl.vu.datalayer.hbase.exceptions.QuadSizeException;
import nl.vu.datalayer.hbase.id.BaseId;
import nl.vu.datalayer.hbase.id.DataPair;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TableInsertResourceToTriple extends ResourceToTriple.ResourceToTripleReducer {

	private HTable spocTable = null;

	@Override
	protected void cleanup(Reducer.Context context) throws IOException, InterruptedException {
		spocTable.close();
	}

	@Override
	protected void setup(Reducer.Context context) throws IOException, InterruptedException {
		String suffix = context.getConfiguration().get("SUFFIX");
		spocTable = new HTable(HBaseConfiguration.create(), HBPrefixMatchSchema.TABLE_NAMES[HBPrefixMatchSchema.SPOC]+suffix);
		spocTable.setAutoFlush(false);
		spocTable.setWriteBufferSize(40*1024*1024);
		spocTable.setRegionCachePrefetch(spocTable.getName(),true);
	}

	@Override
	public void reduce(BaseId tripleId, Iterable<DataPair> values, Context context) throws IOException, InterruptedException {
		try {
			buildKey(values);
		} catch (QuadSizeException e) {
			System.err.println(e.getMessage());
			return;
		}

		try{
			Put put = new Put(outValues);
			put.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, null);

			spocTable.put(put);
		}
		catch (IOException e){
			System.err.println("Problems with put operation: "+e.getMessage());
		}
	}


}
