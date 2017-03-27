package org.buaa.nlsde.jianglili.query.hbaserdf.utils;

import nl.vu.datalayer.hbase.HBaseClientSolution;
import nl.vu.datalayer.hbase.HBaseFactory;
import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;
import nl.vu.examples.BSBMQueries;
import nl.vu.examples.RunJenaHBase;
import nl.vu.jena.graph.HBaseGraph;
import nl.vu.jena.sparql.engine.main.HBaseStageGenerator;
import nl.vu.jena.sparql.engine.optimizer.HBaseOptimize;
import nl.vu.jena.sparql.engine.optimizer.HBaseTransformFilterPlacement;
import org.apache.jena.graph.Graph;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryExecutionBase;
import org.apache.jena.sparql.engine.main.QueryEngineMain;
import org.apache.jena.sparql.engine.main.StageBuilder;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import java.io.IOException;

/**
 * Created by jianglili on 2017/2/3.
 */
public class SPARQLtest {


    public static void main(String[] args) throws SailException, RepositoryException {
        args="-f E:\\benchmarks\\LUBM\\query28-ub\\query1.rq".split(" ");
        //nl.vu.datalayer.hbase.SPARQLQuery.main(args);


        RunJenaHBase.main(args);


    }


}
