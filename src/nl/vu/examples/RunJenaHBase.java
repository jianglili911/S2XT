package nl.vu.examples;

import nl.vu.datalayer.hbase.Quorum;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
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

import java.io.IOException;
import java.util.Iterator;

public class RunJenaHBase {

	public static void main(String[] args) {
		Quorum.confFile="config"+"_test"+".properties";
		String queryfile="E:\\benchmarks\\LUBM\\query28-ub\\query1.rq";

		HBaseConnection con;
		try {
			con = HBaseConnection.create(HBaseConnection.NATIVE_JAVA);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		HBaseClientSolution hbaseSol = HBaseFactory.getHBaseSolution(
				"local-"+ HBPrefixMatchSchema.SCHEMA_NAME, con, null);

		Graph g = new HBaseGraph(hbaseSol, HBaseGraph.CACHING_ON);
		Model model = ModelFactory.createModelForGraph(g);

		runSPARQLQuery(queryfile, model);
	}

	public static void runSPARQLQuery(String queryfile, Model model) {
		String queryString = queryfile;

		System.out.println("Query: \""+queryString+" \"");
		Query query = QueryFactory.read(queryfile);
		HBaseStageGenerator hbaseStageGenerator = new HBaseStageGenerator();
		StageBuilder.setGenerator(ARQ.getContext(), hbaseStageGenerator) ;
		
		ARQ.getContext().set(ARQConstants.sysOptimizerFactory, HBaseOptimize.hbaseOptimizationFactory);
		ARQ.getContext().set(ARQ.optFilterPlacement, new HBaseTransformFilterPlacement());
		
		QueryExecutionBase qexec = (QueryExecutionBase)QueryExecutionFactory.create(query, model);
		
		try {
			executeSelect(qexec);
			//executeDescribe(qexec, model);
		} finally {
			qexec.close();
		}
	}
	
	private static void executeDescribe(QueryExecutionBase qexec, Model model){
		Iterator<Triple> it = qexec.execDescribeTriples();
		while (it.hasNext()){
			System.out.println(it.next());
		}
	}

	private static void executeSelect(QueryExecutionBase qexec) {
		ResultSet results;
		results = qexec.execSelect();

		System.out.println("Solutions: "+results.getRowNumber());
		while (results.hasNext()){
			QuerySolution solution = results.next();
			System.out.println(solution.toString());
		}
	}

	public static void printStatements(Model model) {
		StmtIterator iter = model.listStatements();
        try {
            while ( iter.hasNext() ) {
                Statement stmt = iter.next();
                
                Resource s = stmt.getSubject();
                Resource p = stmt.getPredicate();
                RDFNode o = stmt.getObject();
                
                System.out.print(s+" "+p+" "+o);
                
                /*if ( s.isURIResource() ) {
                    System.out.print("URI");
                } else if ( s.isAnon() ) {
                    System.out.print("blank");
                }
                
                if ( p.isURIResource() ) 
                    System.out.print(" URI ");
                
                if ( o.isURIResource() ) {
                    System.out.print("URI");
                } else if ( o.isAnon() ) {
                    System.out.print("blank");
                } else if ( o.isLiteral() ) {
                    System.out.print("literal");
                }*/
                
                System.out.println();                
            }
        } finally {
            if ( iter != null ) iter.close();
        }
	}

	private static ResultSet query(Model model, String queryString) {
		Query query = QueryFactory.create(queryString);
		QueryExecution qexec = QueryExecutionFactory.create(query, model);
		
		ResultSet results;
		try {
			results = qexec.execSelect();
			//ResultSetFormatter.asRDF(result, results);
		} finally {
			qexec.close();
		}
		return results;
	}

}
