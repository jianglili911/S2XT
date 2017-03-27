package nl.vu.datalayer.hbase.coprocessor;

import nl.vu.datalayer.hbase.bulkload.*;
import nl.vu.datalayer.hbase.id.BaseId;
import nl.vu.datalayer.hbase.id.DataPair;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class CoprocessorBulkLoad extends AbstractPrefixMatchBulkLoad {

	private String coprocessorPath;
	
	public CoprocessorBulkLoad(Path input, String outputPath,
							   String schemaSuffix, boolean onlyTriples, int numberOfSlaveNodes,
							   String coprocessorPath) {
		super(input,  outputPath, schemaSuffix, onlyTriples, numberOfSlaveNodes);
		this.coprocessorPath = coprocessorPath;
	}

	@Override
	protected void bulkLoadQuadTables(Path convertedTripletsPath) throws Exception {
		//do nothing
	}
	
	protected HBPrefixMatchSchema createPrefixMatchSchema() {
		return new HBPrefixMatchSchema(con, schemaSuffix, coprocessorPath);
	}

	private Job createResourceToTripleJob(Path input, Path output) throws IOException {
		Configuration conf = new Configuration();
		
		int childJVMSize = getChildJVMSize(conf);

		
		conf.set("SUFFIX", schemaSuffix);
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		
		Job j = new Job(conf);
		j.setJobName("TableInsertResourceToTriple");
		
		int reduceTasks = (int)(1.75*(double)numberOfSlaveNodes*(double)TASK_PER_NODE);
		j.setNumReduceTasks(reduceTasks);
	
		j.setJarByClass(BulkLoad.class);
		j.setMapperClass(ResourceToTriple.ResourceToTripleMapper.class);
		j.setReducerClass(TableInsertResourceToTriple.class);
	
		j.setMapOutputKeyClass(BaseId.class);
		j.setMapOutputValueClass(DataPair.class);
		j.setOutputKeyClass(NullWritable.class);
		j.setOutputValueClass(ImmutableBytesWritable.class);
		j.setInputFormatClass(SequenceFileInputFormat.class);
		j.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileInputFormat.setInputPaths(j, input);
		SequenceFileOutputFormat.setOutputPath(j, output);
	
		return j;
	}

	protected void runResourceToTripleJob(Path resourceIds, Path convertedTripletsPath) throws IOException, InterruptedException, ClassNotFoundException {
		if (!fs.exists(convertedTripletsPath)){
			long start = System.currentTimeMillis();
			Job j2 = createResourceToTripleJob(resourceIds, convertedTripletsPath);
			j2.waitForCompletion(true);
			long j2Time = System.currentTimeMillis() - start;
			System.out.println("[Time] Second pass finished: " + j2Time + " ms");
		}
		
		runCoprocessors();
		long start = System.currentTimeMillis();
		prefMatchSchema.flushSecondaryIndexTables();
		System.out.println("[Time] Flushing secondary index tables took: "+(System.currentTimeMillis()-start)+" ms");
		//flushCoprocessorBuffers((HBPrefixMatchSchema.TABLE_NAMES[HBPrefixMatchSchema.SPOC]+schemaSuffix).getBytes());
	}

	private void runCoprocessors() throws IOException {
		long start = System.currentTimeMillis();
		con.getConfiguration().setInt("hbase.rpc.timeout", 0);
		HTable spoc = (HTable)con.getTable(HBPrefixMatchSchema.TABLE_NAMES[HBPrefixMatchSchema.SPOC]+schemaSuffix);
		spoc.setOperationTimeout(0);


		try {
//			spoc.coprocessorExec(PrefixMatchProtocol.class, null, null,
//											Batch.forMethod(PrefixMatchProtocol.class, "generateSecondaryIndex"));
//			spoc.coprocessorService(PrefixMatchProtocol.class,null,null,
//					                 Batch.Call< PrefixMatchProtocol.class,>);

		} catch (Exception e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
		long coprocLoadingTime = System.currentTimeMillis()-start;
		System.out.println("[Time] Coprocessor loading finished in: "+coprocLoadingTime+" ms");
	}

	public static void main(String[] args) {
		try {
			if (args.length != 4){
				System.out.println("Usage: bulkLoad <inputPath> <inputSizeEstimate in MB> <outputPath> "
										+"<coprocessorPath>");
				return;
			}
			
			Properties prop = new Properties();
			prop.load(new FileInputStream("config.properties"));
			
			String schemaSuffix = prop.getProperty(HBPrefixMatchSchema.SUFFIX_PROPERTY, "");
			boolean onlyTriples = Boolean.parseBoolean(prop.getProperty(HBPrefixMatchSchema.ONLY_TRIPLES_PROPERTY, ""));
			int slaveNodes = Integer.parseInt(prop.getProperty(HBPrefixMatchSchema.NUMBER_OF_SLAVE_NODES_PROPERTY, ""));
			
			CoprocessorBulkLoad bulkLoad = new CoprocessorBulkLoad(new Path(args[0]),
									args[1],
									schemaSuffix,
									onlyTriples,
									slaveNodes,
									args[2]);
			
			bulkLoad.run();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	
}
