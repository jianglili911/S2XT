package org.buaa.nlsde.jianglili.reasoningquery;

import com.google.common.collect.Lists;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;

import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static java.util.stream.Collectors.joining;

/**
 * Created by jianglili on 2016/5/25.
 */
public class MainQueryYago {

    public static void main(String[] args) throws Exception {

        String  scehmafile="file:e://benchmarks//yago//yagoTaxonomy.ttl";
        String  queryfile="e://benchmarks//yago//YAGO_Queries.txt";
        String queries = Files.lines(Paths.get(queryfile), StandardCharsets.UTF_8).collect(joining("\n"));
        Iterator<String> querysPre= Arrays.asList(queries.split("Q\\d+:\\n")).
                stream().filter(s -> s.startsWith("###")||s.startsWith("####"))
                .filter(s->s.contains("WHERE"))
                .map(s -> s.replace("\n"," ").replace("\t","").replace("#",""))
                .map(s->s.split("}")[0])
                .map(s->s+"}").iterator();
        List<String> querys= Lists.newArrayList(querysPre);
        query(scehmafile, querys);

    }
    public static void query(String scehmafile,List<String> queries) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {
        List<Op> opListPre=new ArrayList<Op>();
        // Parse the quwey file
        for(String querystring:queries) {

//            System.out.println(querystring);
            Query query = QueryFactory.create(querystring);
            // Generate algebra, from query to Op
            Op op = Algebra.compile(query);
//             System.out.println(op);
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
    }
}
