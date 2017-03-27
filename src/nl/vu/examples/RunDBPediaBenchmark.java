package nl.vu.examples;

import nl.vu.datalayer.hbase.Quorum;
import org.apache.jena.graph.Graph;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.engine.QueryExecutionBase;
import org.apache.jena.sparql.engine.main.StageBuilder;
import nl.vu.datalayer.hbase.HBaseClientSolution;
import nl.vu.datalayer.hbase.HBaseFactory;
import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;
import nl.vu.jena.graph.HBaseGraph;
import nl.vu.jena.sparql.engine.main.HBaseStageGenerator;
import nl.vu.jena.sparql.engine.optimizer.HBaseOptimize;
import nl.vu.jena.sparql.engine.optimizer.HBaseTransformFilterPlacement;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

public class RunDBPediaBenchmark {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
	//	Quorum.confFile="config"+"_test"+".properties";
		HBaseConnection con;
		try {
			con = HBaseConnection.create(HBaseConnection.NATIVE_JAVA);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		HBaseClientSolution hbaseSol = HBaseFactory.getHBaseSolution(
				"local-"+ HBPrefixMatchSchema.SCHEMA_NAME, con, null);

		Graph g = new HBaseGraph(hbaseSol, HBaseGraph.CACHING_OFF);
		Model model = ModelFactory.createModelForGraph(g);
		
		prepareHBaseEngine();
		
		try {
			
			int warmupRuns = Integer.parseInt(args[1]);
			int runs = Integer.parseInt(args[2]);
			
			ArrayList<ArrayList<QueryMetrics>> recordedTimes =  new ArrayList<ArrayList<QueryMetrics>>();
			Query []queryMix = createQueryMix(args[0], recordedTimes);
			
			for (int i = 0; i < warmupRuns; i++) {
				runQueryMix(model, queryMix, recordedTimes);
			}
			
			System.out.println("--------- Warmup finished ------");
			
			for (int i = 0; i < runs; i++) {
				runQueryMix(model, queryMix, recordedTimes);
			}
			
			System.out.println("--------- Benchmark fished ------");
			
			writeResults(recordedTimes);
		}
		catch(Exception e){
			e.printStackTrace();
		}

	}

	private static void writeResults(ArrayList<ArrayList<QueryMetrics>> recordedTimes) throws IOException {
		FileWriter writer1 = new FileWriter("Runtimes");
		FileWriter writer2 = new FileWriter("ResultCount");
		for (int i = 0; i < recordedTimes.size(); i++) {
			writer1.write("Q"+i+": ");
			writer2.write("Q"+i+": ");
			for (QueryMetrics queryMetric : recordedTimes.get(i)) {
				writer1.write(String.format("%.3f ", queryMetric.runTime));
				writer2.write(String.format("%d ", queryMetric.resultCount));
			}
		}
		writer1.close();
		writer2.close();
	}
	
	private static int executeSelect(QueryExecutionBase qexec) {
		ResultSet results;
		results = qexec.execSelect();
		int resultCount = 0;
		while (results.hasNext()){
			QuerySolution solution = results.next();
			resultCount++;
			//System.out.println(solution.toString());
		}
		return resultCount;
	}
	
	public static Query []createQueryMix(String directoryPath, ArrayList<ArrayList<QueryMetrics>> recordedTimes) throws FileNotFoundException{
		File directory = new File(directoryPath);
		Query []queries = new Query[directory.listFiles().length];
		int i = 0;
		File []files = directory.listFiles();
		Arrays.sort(files, new Comparator<File>(){
			@Override
			public int compare(File o1, File o2) {
				return o1.getName().compareTo(o2.getName());
			}
			
		});
		
		for (File queryFile : files) {
			recordedTimes.add(new ArrayList<QueryMetrics>());
			try{
				String queryString = readFileAsString(queryFile);
				//String queryString = BSBMQueries.Q08;
				queries[i++] = QueryFactory.create(queryString);
			}
			catch(IOException e){
				e.printStackTrace();
			}		
		}
		
		return queries;
	}
	
	private static String readFileAsString(File file) throws IOException {
        StringBuffer fileData = new StringBuffer();
        BufferedReader reader = new BufferedReader(
                new FileReader(file));
        char[] buf = new char[1024];
        int numRead=0;
        while((numRead=reader.read(buf)) != -1){
            String readData = String.valueOf(buf, 0, numRead);
            fileData.append(readData);
        }
        reader.close();
        return fileData.toString();
    }

	public static void runQueryMix(Model model, Query []queries, ArrayList<ArrayList<QueryMetrics>> metrics) {
		//String queryString = BSBMQueries.Q5;
		
		for (int i = 0; i < queries.length; i++) {
			QueryExecutionBase qexec = (QueryExecutionBase)QueryExecutionFactory.create(queries[i], model);
			try {
				long start = System.nanoTime();
				int resultCount = executeSelect(qexec);
				long end = System.nanoTime();
				metrics.get(i).add(new QueryMetrics((double)(end-start)/1000.0, resultCount));
				//executeDescribe(qexec, model);
			} finally {
				qexec.close();
			}
		}
		
		
		
		
	}

	private static void prepareHBaseEngine() {
		HBaseStageGenerator hbaseStageGenerator = new HBaseStageGenerator();
		StageBuilder.setGenerator(ARQ.getContext(), hbaseStageGenerator) ;
		
		ARQ.getContext().set(ARQConstants.sysOptimizerFactory, HBaseOptimize.hbaseOptimizationFactory);
		ARQ.getContext().set(ARQ.optFilterPlacement, new HBaseTransformFilterPlacement());
	}
	
	public static class QueryMetrics{
		double runTime;
		int resultCount;
		public QueryMetrics(double runTime, int resultCount) {
			super();
			this.runTime = runTime;
			this.resultCount = resultCount;
		}
	}
}
