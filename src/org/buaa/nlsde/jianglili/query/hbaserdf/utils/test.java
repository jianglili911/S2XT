package org.buaa.nlsde.jianglili.query.hbaserdf.utils;

import nl.vu.jena.sparql.engine.main.HBaseStageGenerator;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFReader;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryExecutionBase;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.engine.main.QC;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by jianglili on 2017/2/4.
 */
public class test {

    public static void main(String[] args) {

        System.out.println(System.currentTimeMillis());
        System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));// new Date()为获取当前系统时间
        String queryfile="E:\\benchmarks\\LUBM\\query28-ub\\query1.rq";
        String datafile="file:"+ "E://benchmarks//LUBM//univ-benchQL.owl";

        Query query = QueryFactory.read(queryfile);
        Model model= RDFDataMgr.loadModel(datafile);
        QueryExecutionBase qexec = (QueryExecutionBase) QueryExecutionFactory.create(query, model);
        HBaseStageGenerator hBaseStageGenerator=new HBaseStageGenerator();

        //1 QC中注册
        OpExecutorFactory customExecutorFactory = new OpExecutorFactory() {
            @Override
            public OpExecutor create(ExecutionContext execCxt) {
                return null;
            }
        };
        QC.setFactory(ARQ.getContext(), customExecutorFactory) ;

        //2 executionContext中注册
        // Execute an operation with a different OpExecution Factory
        // New context.
        ExecutionContext execCxt;




    }
}
