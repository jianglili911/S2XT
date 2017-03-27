package nl.vu.datalayer.hbase.bulkload;

import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.connection.NativeJavaConnection;
import nl.vu.datalayer.hbase.id.DataPair;
import nl.vu.datalayer.hbase.id.HBaseValue;
import nl.vu.datalayer.hbase.id.TypedId;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractPrefixMatchBulkLoad {
	
	private static final long DEFAULT_BLOCK_SIZE = 134217728;
	private  Table string2Id = null;
	private  Table id2String = null;
	/**
	 * Cluster parameters used to estimate number of reducers 
	 */
	public  int TASK_PER_NODE = 2;
	/**
	 * Estimate of a quad size  
	 */
	
	public  int ELEMENTS_PER_QUAD = 4;
	/**
	 * Estimate of imbalance of data  distributed across reducers
	 * 1.0 = data equally distributed across reducers 
	 */
	public  double LOAD_BALANCER_FACTOR = 1.2;
	public  long totalStringCount;
	public  long numericalCount;
	public  long literalCount;
	public  long bNodeCount;
	public  int tripleToResourceReduceTasks;
	public  String schemaSuffix = "";
	protected Path input;
	protected  String outputPath;
	protected  byte rdfUnitType;
	protected  NativeJavaConnection con;
	protected  LoadIncrementalHFiles bulkLoad;
	protected FileSystem fs;
	protected long inputSplitSize;
	protected HBPrefixMatchSchema prefMatchSchema;
	protected int numberOfSlaveNodes;

	public AbstractPrefixMatchBulkLoad(Path input, String outputPath, String schemaSuffix, boolean onlyTriples, int numberOfSlaveNodes) {
		this.schemaSuffix = schemaSuffix;
		this.input = input;
		this.outputPath = outputPath;
		if (onlyTriples)
			rdfUnitType = RDFUnit.TRIPLE;
		else
			rdfUnitType = RDFUnit.QUAD;
		this.numberOfSlaveNodes = numberOfSlaveNodes;
		this.tripleToResourceReduceTasks=numberOfSlaveNodes;

	}

	protected void run() throws IOException, Exception, InterruptedException, ClassNotFoundException {
		long globalStartTime = System.currentTimeMillis();
		Path convertedTripletsPath = new Path(outputPath+ResourceToTriple.TEMP_TRIPLETS_DIR);
		Path idStringAssocInput = new Path(outputPath+"/"+QuadBreakDown.ID2STRING_DIR);
		
		con = (NativeJavaConnection)HBaseConnection.create(HBaseConnection.NATIVE_JAVA);
		prefMatchSchema = createPrefixMatchSchema();
		prefMatchSchema.createCounterTable(con.getAdmin());
		
		Path resourceIds = new Path(outputPath+"/"+QuadBreakDown.RESOURCE_IDS_DIR);
		fs = FileSystem.get(con.getConfiguration());

		inputSplitSize = con.getConfiguration().getLong("dfs.blocksize", DEFAULT_BLOCK_SIZE);
		System.out.println("Input split block size: "+inputSplitSize);
		//second timer 0 : 0  create table  String2Id_lubmtest  Id2String_lubmtest  POCS_lubmtest  OSPC_lubmtest SPOC_lubmtest  generate idStringAssoc resourceIds
		runTripleToResourceJob(idStringAssocInput, resourceIds, prefMatchSchema);
		
		//childOpts -Xmx200m ChildJVM 200  Second pass finished   SECOND PASS-----------------------------------------------
		runResourceToTripleJob(resourceIds, convertedTripletsPath);

		//begin load data
		bulkLoad = new LoadIncrementalHFiles(con.getConfiguration());

		bulkLoadIdStringMappingTables(idStringAssocInput);
		bulkLoadQuadTables(convertedTripletsPath);	//SECOND PASS-----------------------------------------------
		runResourceToTripleJob(resourceIds, convertedTripletsPath);

		bulkLoad = new LoadIncrementalHFiles(con.getConfiguration());

		bulkLoadIdStringMappingTables(idStringAssocInput);
		bulkLoadQuadTables(convertedTripletsPath);
//
		con.close();
		long globalEndTime = System.currentTimeMillis();
		System.out.println("[Time] Total time: "+(globalEndTime-globalStartTime)+" ms");
		
		//TODO prefMatchSchema.warmUpBlockCache();
	}

	protected abstract void runResourceToTripleJob(Path resourceIds, Path convertedTripletsPath) throws IOException, InterruptedException, ClassNotFoundException;
     //Counter updated for partition i  Closed file writer
	private void runTripleToResourceJob(Path idStringAssocInput, Path resourceIds, HBPrefixMatchSchema schema) throws IOException, InterruptedException, ClassNotFoundException {
		if (!fs.exists(resourceIds)) {
			long start = System.currentTimeMillis();
			Job j1 = createTripleToResourceJob(input, resourceIds);

			j1.waitForCompletion(true);

			//move side effect files out of TripleToResource output directory
			moveIdStringAssocDirectory(resourceIds, idStringAssocInput);

			long firstJob = System.currentTimeMillis() - start;
			System.out.println("[Time] First pass finished in: " + firstJob + " ms");

			retrieveTripleToResourceCounters(j1);
			createSchemaFromCounters(schema);     //create tables
		}
		else{//the output directory exists - we skip this job and read the counters from file
			createSchemaFromFile(schema);
		}
	}
	public  void moveIdStringAssocDirectory(Path resourceIds, Path id2StringInput) throws IOException {
		Path source = new Path(resourceIds, QuadBreakDown.ID2STRING_DIR);
		fs.rename(source, id2StringInput);
	}
	public Job createTripleToResourceJob(Path input, Path output) throws IOException {
		Configuration conf = new Configuration();
		conf.set("schemaSuffix", schemaSuffix);
		Job j = new Job(conf);
		j.setJobName("TripleToResource");

		System.out.println("Number of reduce tasks: "+tripleToResourceReduceTasks);

		j.setJarByClass(BulkLoad.class);
		j.setMapperClass(QuadBreakDown.TripleToResourceMapper.class);
		j.setReducerClass(QuadBreakDown.TripleToResourceReducer.class);

		j.setOutputKeyClass(TypedId.class);
		j.setOutputValueClass(DataPair.class);

		j.setMapOutputKeyClass(HBaseValue.class);
		j.setMapOutputValueClass(DataPair.class);

		j.setInputFormatClass(TextInputFormat.class);
		j.setOutputFormatClass(SequenceFileOutputFormat.class);

		TextInputFormat.setInputPaths(j, input);
		SequenceFileOutputFormat.setOutputPath(j, output);

		j.setNumReduceTasks(tripleToResourceReduceTasks);

		return j;
	}


	protected void createSchemaFromCounters(HBPrefixMatchSchema prefMatchSchema) throws IOException {
		long startPartition = 0;//TODO HBPrefixMatchSchema.updateLastCounter(tripleToResourceReduceTasks, con.getConfiguration(), schemaSuffix)+1;
		//long startPartition = HBPrefixMatchSchema.getLastCounter(con.getConfiguration())+1;
		//System.out.println(totalStringCount+" : "+numericalCount);
		
		prefMatchSchema.setTableSplitInfo(totalStringCount, numericalCount, 
				tripleToResourceReduceTasks, startPartition, rdfUnitType==RDFUnit.TRIPLE);
		prefMatchSchema.create();
	}
	
	protected void createSchemaFromFile(HBPrefixMatchSchema prefMatchSchema) throws IOException {
		long startPartition = 0;
		System.out.println(totalStringCount+" : "+numericalCount);
		buildCountersFromFile();
		
		prefMatchSchema.setTableSplitInfo(totalStringCount, numericalCount, 
				tripleToResourceReduceTasks, startPartition, rdfUnitType==RDFUnit.TRIPLE);
		prefMatchSchema.create();
	}
	
	
	
	protected  final void bulkLoadIdStringMappingTables(Path idStringAssocInput) throws Exception, IOException, InterruptedException, ClassNotFoundException {
		Path string2IdOutput = new Path(outputPath+StringIdAssoc.STRING2ID_DIR);
		Path id2StringOutput = new Path(outputPath+StringIdAssoc.ID2STRING_DIR);
		
		//String2Id PASS----------------------------------------------
		if (!fs.exists(string2IdOutput)) {
			long start = System.currentTimeMillis();
			Job j3 = createString2IdJob(con, idStringAssocInput, string2IdOutput);
			j3.waitForCompletion(true);
			System.out.println("[Time] String2Id MR finished in: " + (System.currentTimeMillis() - start));
			//string2Id = con.getTable(HBPrefixMatchSchema.STRING2ID+schemaSuffix);//TODO add when reading from file
			doTableBulkLoad(string2IdOutput, string2Id, con);
		}
		
		System.out.println("Finished bulk load for String2Id table");//=====================//=============
		
		if (!fs.exists(id2StringOutput)) {
			long start = System.currentTimeMillis();
			Job j4 = createId2StringJob(con, idStringAssocInput, id2StringOutput);
			j4.waitForCompletion(true);
			System.out.println("[Time] Id2String MR finished in: " + (System.currentTimeMillis() - start));

			//id2String = con.getTable(HBPrefixMatchSchema.ID2STRING+schemaSuffix);TODO add when reading from file
			doTableBulkLoad(id2StringOutput, id2String, con);
		} 
		System.out.println("Finished bulk load for Id2String table");//=====================//==================================
	}



	public Job createString2IdJob(HBaseConnection con, Path input, Path output) throws Exception {
		JobConf conf = new JobConf();

		Job j = new Job(conf);
	
		j.setJobName(HBPrefixMatchSchema.STRING2ID+schemaSuffix);
		j.setJarByClass(BulkLoad.class);
		j.setMapperClass(StringIdAssoc.String2IdMapper.class);
		j.setMapOutputKeyClass(ImmutableBytesWritable.class);
		j.setMapOutputValueClass(Put.class);
		j.setInputFormatClass(SequenceFileInputFormat.class);
		j.setOutputFormatClass(HFileOutputFormat.class);
	
		SequenceFileInputFormat.setInputPaths(j, input);
		HFileOutputFormat.setOutputPath(j, output);
	
		((NativeJavaConnection)con).getConfiguration().setInt("hbase.rpc.timeout", 0);
		string2Id = con.getTable(HBPrefixMatchSchema.STRING2ID+schemaSuffix);
	
		HFileOutputFormat.configureIncrementalLoad(j, (HTable)string2Id);
		return j;
	}

	protected int getChildJVMSize(Configuration conf) {
		String childOptsString = conf.get("mapred.child.java.opts");
		System.out.println("ChildOpts: "+childOptsString);
		Pattern p = Pattern.compile("-Xmx([0-9]*)m");
		Matcher m =p.matcher(childOptsString);
		int childJVMSize = 1024;
		if (m.find()){
			childJVMSize = Integer.parseInt(m.group(1));
		}
		System.out.println("ChildJVM: "+childJVMSize);
		return childJVMSize;
	}

	public Job createId2StringJob(HBaseConnection con, Path input, Path output) throws Exception {
		JobConf conf = new JobConf();
		int childJVMSize = getChildJVMSize(conf);
		conf.setInt("hbase.mapreduce.hfileoutputformat.blocksize", 8*1024);
		Job j = new Job(conf);
	
		j.setJobName(HBPrefixMatchSchema.ID2STRING+schemaSuffix);
		j.setJarByClass(BulkLoad.class);
		j.setMapperClass(StringIdAssoc.Id2StringMapper.class);
		j.setMapOutputKeyClass(ImmutableBytesWritable.class);
		j.setMapOutputValueClass(Put.class);
		j.setInputFormatClass(SequenceFileInputFormat.class);
		j.setOutputFormatClass(HFileOutputFormat.class);
	
		SequenceFileInputFormat.setInputPaths(j, input);
		HFileOutputFormat.setOutputPath(j, output);
	
		((NativeJavaConnection)con).getConfiguration().setInt("hbase.rpc.timeout", 0);
		id2String = con.getTable(HBPrefixMatchSchema.ID2STRING+schemaSuffix);
	
		HFileOutputFormat.configureIncrementalLoad(j, (HTable)id2String);
	
		return j;
	}

	public  void retrieveTripleToResourceCounters(Job j1) throws IOException {

		
		Counters counters = j1.getCounters();
		CounterGroup numGroup = counters.getGroup(QuadBreakDown.TripleToResourceReducer.NUMERICAL_GROUP);
		totalStringCount = numGroup.findCounter("NonNumericals").getValue();
		numericalCount = numGroup.findCounter("Numericals").getValue();
		
		CounterGroup elemsGroup = counters.getGroup(QuadBreakDown.TripleToResourceReducer.ELEMENT_TYPE_GROUP);
		literalCount = elemsGroup.findCounter("Literals").getValue();
		bNodeCount = elemsGroup.findCounter("Blanks").getValue();
		


		//save counter values to file
		FileWriter file = new FileWriter("Counters");
		file.write(Long.toString(totalStringCount)+"\n");
		file.write(Long.toString(numericalCount)+"\n");
		file.write(Integer.toString(tripleToResourceReduceTasks)+"\n");

		file.close();
	}

	public  void buildCountersFromFile() throws IOException {
		//sufixCounters = new TreeMap();
		FileReader file2 = new FileReader("Counters");
		BufferedReader reader = new BufferedReader(file2);
		totalStringCount = Long.parseLong(reader.readLine());
		numericalCount = Long.parseLong(reader.readLine());
		tripleToResourceReduceTasks = Integer.parseInt(reader.readLine());

	}



	protected  void doTableBulkLoad(Path dir, Table table, NativeJavaConnection con) throws InterruptedException, TableNotFoundException, IOException {
		long start = System.currentTimeMillis();
		bulkLoad.doBulkLoad(dir, (HTable) table);
		long bulkTime = System.currentTimeMillis() - start;
		System.out.println("[Time] "+table.getTableDescriptor().getNameAsString() + " bulkLoad time: " + bulkTime + " ms");
	}
	


	protected abstract HBPrefixMatchSchema createPrefixMatchSchema();
	
	protected abstract void bulkLoadQuadTables(Path convertedTripletsPath) throws Exception;

}
