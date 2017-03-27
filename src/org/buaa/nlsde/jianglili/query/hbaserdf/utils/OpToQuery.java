package org.buaa.nlsde.jianglili.query.hbaserdf.utils;

import nl.vu.datalayer.hbase.Quorum;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.syntax.Element;
import org.buaa.nlsde.jianglili.query.hbaserdf.OpToQueryTrans.AlgebraTransformer;
import org.buaa.nlsde.jianglili.reasoningquery.QueryRewrting;
import org.buaa.nlsde.jianglili.reasoningquery.conceptExtract.Concept;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;

import java.io.FileNotFoundException;

/**
 * Created by jianglili on 2017/2/4.
 */
public class OpToQuery {
    public static void main(String[] args) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {
        //load the schema
        Concept concept= QueryRewrting.initSchema("file:"+ "E://benchmarks//LUBM//univ-benchQL-ub.owl",0);
        String querydir="E:\\benchmarks\\LUBM\\query28-ub";
        int[]  nums=new int[21];
        for(int i=1;i<=18;i++)
            nums[i-1]=i;
        nums[18]=20; nums[19]=22; nums[20]=28;
        for(int i=0;i<nums.length;i++) {
             queryParse(querydir + "\\query" + nums[i] + ".rq",concept);
         //     queryParse(querydir + "\\query" + nums[i] + ".rq",concept);
        }

    }

    public static void queryParse(String queryfile,Concept concept) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {
        System.out.println("query: "+queryfile+"datastore:  "+ Quorum.confFile);
        long startRewrite = System.currentTimeMillis();
        Query query = QueryFactory.read(queryfile);
        Op opRoot = Algebra.compile(query) ;
        // rewrite the op
        Op opRootRewrite= QueryRewrting.transform(opRoot, concept);
        System.out.println("op to String: \n"+opRootRewrite.toString());
        System.out.println("query to String before: \n"+query.toString());
        Element element=new AlgebraTransformer().transform(opRootRewrite);
        query.setQueryPattern(element);
        System.out.println("query to String after: \n"+query.toString());
         // query.setQueryPattern(opRootRewrite.toString());
//        Query q = QueryFactory.make();
//        q.setQueryPattern(body);                               // Set the body of the query to our group
//        q.setQuerySelectType();                                // Make it a select query
//        q.addResultVar("s");                                   // Select ?s

    }
}
