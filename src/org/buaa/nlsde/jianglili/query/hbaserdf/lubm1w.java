package org.buaa.nlsde.jianglili.query.hbaserdf;

import de.tf.uni.freiburg.sparkrdf.constants.Const;
import nl.vu.datalayer.hbase.HBaseClientSolution;
import nl.vu.datalayer.hbase.HBaseFactory;
import nl.vu.datalayer.hbase.Quorum;
import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;
import nl.vu.jena.graph.HBaseGraph;
import nl.vu.jena.sparql.engine.main.HBaseStageGenerator;
import nl.vu.jena.sparql.engine.optimizer.HBaseOptimize;
import nl.vu.jena.sparql.engine.optimizer.HBaseTransformFilterPlacement;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.QueryExecutionBase;
import org.apache.jena.sparql.engine.iterator.QueryIteratorCheck;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.StageBuilder;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.tdb.solver.QueryEngineTDB;
import org.apache.jena.tdb.solver.QueryIterTDB;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.buaa.nlsde.jianglili.query.hbaserdf.OpToQueryTrans.AlgebraTransformer;
import org.buaa.nlsde.jianglili.query.jenaSDB.lubm1Test;
import org.buaa.nlsde.jianglili.reasoningquery.QueryRewrting;
import org.buaa.nlsde.jianglili.reasoningquery.conceptExtract.Concept;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by jianglili on 2017/2/3.
 */
public class lubm1w {
    public  static Logger log = Logger.getLogger(lubm1Test.class);
    public static Map<String, Long> operationDuration = new TreeMap<>();
    public static void main(String[] args) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {
        Const.timeFilePath_$eq("E:\\benchmarks\\LUBM\\count\\hbaserdf\\lubm1w.txt");
        Quorum.confFile="config"+"1"+".properties";
        String querydir="E:\\benchmarks\\LUBM\\query28-ub";
        query(querydir);

    }
    public static void query(String  querydir) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {

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



        int[]  nums=new int[21];
        for(int i=1;i<=18;i++)
            nums[i-1]=i;
        nums[18]=20; nums[19]=22; nums[20]=28;
        for(int i=0;i<nums.length;i++) {
            runSPARQLQuery(querydir + "\\query" + nums[i] + ".rq", model,1);
            runSPARQLQuery(querydir + "\\query" + nums[i] + ".rq", model,2);
        }

    }

    public static void runSPARQLQuery(String queryfile, Model model,int qc) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {
        System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"query: "+queryfile+"datastore:  "+Quorum.confFile);
        long startRewrite = System.currentTimeMillis();
        Query query = QueryFactory.read(queryfile);


        HBaseStageGenerator hBaseStageGenerator=new HBaseStageGenerator();
        StageBuilder.setGenerator(ARQ.getContext(),hBaseStageGenerator);


        ARQ.getContext().set(ARQConstants.sysOptimizerFactory, HBaseOptimize.hbaseOptimizationFactory);
        ARQ.getContext().set(ARQ.optFilterPlacement, new HBaseTransformFilterPlacement());

        QueryExecutionBase qexec = (QueryExecutionBase)QueryExecutionFactory.create(query, model);

        //begin
        QueryEngineTDB queryEngineTDB;
        QueryIteratorCheck queryIteratorCheck;
        QueryIterTDB queryIterTDB;
        OpExecutor opExecutor;
        //end


        int resultCount=0;
        try {
         resultCount=executeSelect(qexec);
        } finally {
            qexec.close();
        }
        long endRewrite = System.currentTimeMillis()-startRewrite;
        operationDuration.put("time",endRewrite);
        operationDuration.put("resultCount",Long.valueOf(resultCount));
        printCount(queryfile,qc);
    }
    private static int executeSelect(QueryExecutionBase qexec) {
        ResultSet results;
        results = qexec.execSelect();

   //     System.out.println("Solutions: "+results.getRowNumber());
        int resultCount=0;
        while (results.hasNext()){
            QuerySolution solution = results.next();
          //  System.out.println(solution.toString());
            resultCount++;
        }

        System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+"# solution mappings: "+resultCount );
        return  resultCount;
    }

    public static void printCount(String queryFile,Integer qc){
        // Write the durations and the result count to the given file
        if (Const.timeFilePath() != null) {
            OutputStreamWriter writer;
            BufferedWriter fbw;
            try {
                File f = new File(Const.timeFilePath());
                Boolean exists = f.exists();
                if (!exists) {

                    f.createNewFile();
                    writer = new OutputStreamWriter(new FileOutputStream(f,
                            true), "UTF-8");
                    fbw = new BufferedWriter(writer);
                    fbw.write("Query File;");
                    for (String tag : operationDuration.keySet()) {
                        fbw.write(tag + ";");
                    }
                    fbw.newLine();
                }else {
                    writer = new OutputStreamWriter(new FileOutputStream(f,
                            true), "UTF-8");
                    fbw = new BufferedWriter(writer);
                }
                SimpleDateFormat sfd = new SimpleDateFormat("yyyy-MM-dd  hh:mm:ss");
                fbw.write("["+sfd.format(new Date())+"]"+queryFile + ";");
                for (String tag : operationDuration.keySet()) {
                    fbw.write(operationDuration.get(tag) + ";");
                }
                fbw.newLine();
                if (qc == 2) {
                    fbw.newLine();
                }
                fbw.close();

                // Clear the map for the next iteration
                operationDuration.clear();

            } catch (IOException e) {
                log.log(Level.ERROR, "Couldn't write execution times",
                        e);
            }
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
    private static void executeDescribe(QueryExecutionBase qexec, Model model){
        Iterator<Triple> it = qexec.execDescribeTriples();
        while (it.hasNext()){
            System.out.println(it.next());
        }
    }
}
