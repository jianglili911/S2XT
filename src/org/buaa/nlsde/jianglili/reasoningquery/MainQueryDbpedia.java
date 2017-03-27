package org.buaa.nlsde.jianglili.reasoningquery;


import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.syntax.Element;
import org.buaa.nlsde.jianglili.query.hbaserdf.OpToQueryTrans.AlgebraTransformer;
import org.buaa.nlsde.jianglili.reasoningquery.conceptExtract.Concept;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jianglili on 2016/5/25.
 */
public class MainQueryDbpedia {
    public static void main3(String[] args) throws Exception {

        String  scehmafile="E://benchmarks//LUBM//univ-benchQL-ub.owl";
        String  querydir="E:\\benchmarks\\LUBM\\query28-ub";
        Concept concept=QueryRewrting.initSchema("file:"+ scehmafile,0);
        int[]  nums=new int[21];
        for(int i=1;i<=18;i++)
            nums[i-1]=i;
        nums[18]=20; nums[19]=22; nums[20]=28;
        for(int i=0;i<nums.length;i++) {
            query(querydir + "\\query" + nums[i] + ".rq",concept,1);
        }
    }
    public static void main(String[] args) throws Exception {
        String  scehmafile="E://benchmarks//dbpedia//ontology.ttl";
        String  querydir="e://benchmarks//dbpedia//query2012";
        Concept concept=QueryRewrting.initSchema("file:"+ scehmafile,0);
        for(File f: new File(querydir).listFiles()) {
            query(f.getAbsolutePath(),concept,1);
        }
    }

    public static void query(String queryfile, Concept concept,Integer qc) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {
      System.out.println(queryfile);
        Query query = QueryFactory.read(queryfile);
        Op opRoot = Algebra.compile(query) ;
        String strOpPre=opRoot.toString();
      System.out.println(opRoot.toString());
        // rewrite the op
        Long tp=System.currentTimeMillis();
        Op opRootRewrite= QueryRewrting.transformDBpedia(opRoot, concept,false);


//        Element element=new AlgebraTransformer().transform(opRootRewrite);
//        query.setQueryPattern(element);
        String strOpAfter=opRootRewrite.toString();

        System.out.println(opRootRewrite.toString());
     //  System.out.println(queryfile.substring(queryfile.lastIndexOf("\\")+1)+":"+strOpPre.equals(strOpAfter)+":time:"+(System.currentTimeMillis()-tp));
    //   System.out.println(queryfile.substring(queryfile.lastIndexOf("\\")+1));
        System.out.println(strOpPre.equals(strOpAfter)?"否":"是");
   //     System.out.println((System.currentTimeMillis()-tp));

    }

}
