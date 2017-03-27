package org.buaa.nlsde.jianglili.utils.lubm;

import com.google.common.collect.Lists;
import de.tf.uni.freiburg.sparkrdf.run.QueryExecutor;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.log4j.Logger;
import org.buaa.nlsde.jianglili.reasoningquery.QueryRewrting;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.reducing;

/**
 * Created by jianglili on 2016/5/25.
 */
public class QueryLubm {
    public static void main(String[] args) throws OWLOntologyCreationException, IOException, OWLOntologyStorageException {

        Logger log = Logger.getLogger(QueryLubm.class);
        String queryFile;
        String schemaFile;
        String dataFile;
        if(args.length==3) {
            queryFile = args[0];
            schemaFile=args[1];
            dataFile=args[2];
        }
        else{
            queryFile="d://Users//git//iswc2014-benchmark-master//Ontop//OntopQueries.txt";
            schemaFile="file:d://Users//git//iswc2014-benchmark-master//Ontop//univ-benchQL.owl";;
            dataFile="e://benchmarks//LUBM//datasets//nt";
        }
        String queries = Files.lines(Paths.get(queryFile), StandardCharsets.UTF_8).collect(joining("\n"));
        Iterator<String> querysPre=Arrays.asList(queries.split("#Query\\d+(\\s|\\w)*\\n\\t")).stream().filter(s -> s.startsWith("SELECT"))
                .map(s -> "PREFIX ub:<http://swat.cse.lehigh.edu/onto/univ-bench.owl#>\n" +
                        "PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                        "PREFIX owl:   <http://www.w3.org/2002/07/owl#>\n" +
                        "PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#>\n" +
                        "PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>\n"+s)
                .map(s -> s.replace("\n"," ").replace("\t","")).iterator();
        List<String>  querys= Lists.newArrayList(querysPre);
        List<Op> opNewList=queryRewriting(schemaFile, querys);
        queryData(opNewList,dataFile);

    }
    public static List<Op> queryRewriting(String scehmafile,List<String> queries) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {
        List<Op> opListPre=new ArrayList<Op>();
        // Parse the quwey file
        for(String querystring:queries) {

//          System.out.println(querystring);
            Query query = QueryFactory.create(querystring);
            // Generate algebra, from query to Op
            Op op = Algebra.compile(query);
//            System.out.println(op);
            opListPre.add(op);
        }
        List<Op> opNewList= QueryRewrting.exe(opListPre, scehmafile, 0);
//        for(Op opNew:opNewList)
//           System.out.println(opNew);
        int count=0;
        for(int i=0;i<queries.size();i++)
        {
//            System.out.println(queries.get(i));
//            System.out.println(opListPre.get(i));
//            System.out.println(opNewList.get(i));
            String pre=opListPre.get(i).toString();
            String rewrite=opNewList.get(i).toString();
            if(!pre.equals(rewrite))
            {
                System.out.println(opListPre.get(i));
                System.out.println(opNewList.get(i));
                count++;
            }
        }
        System.out.println("the total query member: "+queries.size());
        System.out.println("the total query diff: "+count);
        return opNewList;

    }

    public static void queryData(List<Op> op, String dataFile) {
        String[]  arg=new String("-i E:/benchmarks/generator/dataset/input " +
                "-mem 3g -q E:/benchmarks/generator/dataset/query/YAGO_Query1.rq" +
                " -o E:/benchmarks/generator/dataset/result/result.nt  " +
                "-t E:\\benchmarks\\generator\\dataset\\result\\count.txt " +
                "-so " +
                "-l " +
                "-jn " +
                "sqX ").split(" ");
     QueryExecutor.main(arg);

    }
}
