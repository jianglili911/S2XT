package org.buaa.nlsde.jianglili.query.hbaserdf;

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
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.QueryExecutionBase;
import org.apache.jena.sparql.engine.main.StageBuilder;
import org.apache.jena.sparql.syntax.Element;
import org.buaa.nlsde.jianglili.query.hbaserdf.OpToQueryTrans.AlgebraTransformer;
import org.buaa.nlsde.jianglili.reasoningquery.QueryRewrting;
import org.buaa.nlsde.jianglili.reasoningquery.conceptExtract.Concept;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jianglili on 2017/2/21.
 */
public class HBaseRDFQueryFactory {

    public static void main(String[] args) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {

        //load the schema
        Concept   concept =QueryRewrting.initSchema("file:"+ "E://benchmarks//LUBM//univ-benchQL-ub.owl",0);
        //load the hbase model  register the methods
        Quorum.confFile="config"+"1"+".properties";
        Model  model= HBaseRDFQueryFactory.init();
        String q=QueryFactory.read("E:\\benchmarks\\LUBM\\query28-ub\\1.rq").toString();
        List<QuerySolution> resultSet=HBaseRDFQueryFactory.runSPARQLQuery(q,model);
        List<QuerySolution> resultSet2=HBaseRDFQueryFactory.runSPARQLQueryRewrite(q,model,concept);
        Iterator<QuerySolution>  resIterator=resultSet.iterator();
        System.out.println(resultSet);

    }

    public static Model init()
    {
        HBaseConnection con;
        try {
            con = HBaseConnection.create(HBaseConnection.NATIVE_JAVA);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        HBaseClientSolution hbaseSol = HBaseFactory.getHBaseSolution(
                "local-"+ HBPrefixMatchSchema.SCHEMA_NAME, con, null);
        Graph g = new HBaseGraph(hbaseSol, HBaseGraph.CACHING_ON);
        Model model = ModelFactory.createModelForGraph(g);
        return model;
    }

    public static  List<QuerySolution> runOpQuery(Op op, Model model, Concept concept) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {

        long startRewrite = System.currentTimeMillis();
        Query query = QueryFactory.create();
        // rewrite the op
        Op opRootRewrite= QueryRewrting.transform(op, concept);
        Element element=new AlgebraTransformer().transform(opRootRewrite);
        query.setQueryPattern(element);
        // end rewrite  op to query
        HBaseStageGenerator hBaseStageGenerator=new HBaseStageGenerator();
        StageBuilder.setGenerator(ARQ.getContext(),hBaseStageGenerator);

        ARQ.getContext().set(ARQConstants.sysOptimizerFactory, HBaseOptimize.hbaseOptimizationFactory);
        ARQ.getContext().set(ARQ.optFilterPlacement, new HBaseTransformFilterPlacement());
        QueryExecutionBase qexec = (QueryExecutionBase) QueryExecutionFactory.create(query, model);

        try {
           return executeSelect(qexec);
        } finally {
            qexec.close();
        }

    }

    public static  List<QuerySolution> runOpRewriteQuery(Op opRewrite, Model model, Concept concept) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {

        long startRewrite = System.currentTimeMillis();
        Query query = QueryFactory.create();
        // rewrite the op
        Element element=new AlgebraTransformer().transform(opRewrite);
        query.setQueryPattern(element);
        // end rewrite  op to query
        HBaseStageGenerator hBaseStageGenerator=new HBaseStageGenerator();
        StageBuilder.setGenerator(ARQ.getContext(),hBaseStageGenerator);

        ARQ.getContext().set(ARQConstants.sysOptimizerFactory, HBaseOptimize.hbaseOptimizationFactory);
        ARQ.getContext().set(ARQ.optFilterPlacement, new HBaseTransformFilterPlacement());
        QueryExecutionBase qexec = (QueryExecutionBase) QueryExecutionFactory.create(query, model);

        try {
            return executeSelect(qexec);
        } finally {
            qexec.close();
        }

    }

    public static  List<QuerySolution> runSPARQLQuery(String queryString, Model model) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {

        long startRewrite = System.currentTimeMillis();
        Query query = QueryFactory.create(queryString);

        //add the hbaseRDF BGP pattern into the HbaseRDF engine
        HBaseStageGenerator hBaseStageGenerator=new HBaseStageGenerator();
        StageBuilder.setGenerator(ARQ.getContext(),hBaseStageGenerator);

        ARQ.getContext().set(ARQConstants.sysOptimizerFactory, HBaseOptimize.hbaseOptimizationFactory);
        ARQ.getContext().set(ARQ.optFilterPlacement, new HBaseTransformFilterPlacement());
        QueryExecutionBase qexec = (QueryExecutionBase) QueryExecutionFactory.create(query, model);

        try {
            return executeSelect(qexec);
        } finally {
            qexec.close();
        }

    }

    public static  List<QuerySolution> runSPARQLQueryRewrite(String queryString, Model model, Concept concept) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {

        long startRewrite = System.currentTimeMillis();
        Query query = QueryFactory.create(queryString);
        Op opRoot = Algebra.compile(query);
        // rewrite the op
        Op opRootRewrite= QueryRewrting.transform(opRoot, concept);
        Element element=new AlgebraTransformer().transform(opRootRewrite);
        query.setQueryPattern(element);
        // end rewrite  op to query

        //add the hbaseRDF BGP pattern into the HbaseRDF engine
        HBaseStageGenerator hBaseStageGenerator=new HBaseStageGenerator();
        StageBuilder.setGenerator(ARQ.getContext(),hBaseStageGenerator);

        ARQ.getContext().set(ARQConstants.sysOptimizerFactory, HBaseOptimize.hbaseOptimizationFactory);
        ARQ.getContext().set(ARQ.optFilterPlacement, new HBaseTransformFilterPlacement());
        QueryExecutionBase qexec = (QueryExecutionBase) QueryExecutionFactory.create(query, model);

        try {
            return executeSelect(qexec);
        } finally {
            qexec.close();
        }

    }

    private static List<QuerySolution> executeSelect(QueryExecutionBase qexec) {
        ResultSet results = qexec.execSelect();
        List<QuerySolution> listResults=new ArrayList<>();
        while (results.hasNext()){
            QuerySolution solution = results.next();
            listResults.add(solution);
        }
        return listResults;
    }
}
