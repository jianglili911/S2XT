package org.buaa.nlsde.jianglili.reasoningquery;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jianglili on 2016/3/23.
 */
public class MainQuery {
    public static void main(String[] args) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {

        String  scehmafile="file:e://benchmarks//people//people.ttl";
        List<String>  queryfiles=new ArrayList<>();
      //  String  queryfile="E://benchmarks//generator//dataset//query//YAGO_Query1.rq";
        String  queryfile="e://benchmarks//people//q2_1.sparql";
        queryfiles.add(queryfile);

//        String  scehmafile="file:e://benchmarks//dbpedia//dbpedia_2014.ttl";
////        String  queryfile="e://benchmarks//dbpedia//query2012//distinct2.rq";
//        List<String>  queryfiles=getDirectoryList("e://benchmarks//dbpedia//query2012/union.rq");

        query(scehmafile, queryfiles);

    }
    public static void query(String scehmafile,List<String> queryfiles) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {
        List<Op> opListPre=new ArrayList<Op>();
        // Parse the quwey file
        for(String queryfile:queryfiles) {
//            System.out.println(queryfile);
            Query query = QueryFactory.read(queryfile);
            // Generate algebra, from query to Op
            Op op = Algebra.compile(query);
//            System.out.println(op);
            opListPre.add(op);
        }
        List<Op> opNewList= QueryRewrting.exe(opListPre, scehmafile, 0);
//        for(Op opNew:opNewList)
//           System.out.println(opNew);
       int count=0;
        for(int i=0;i<queryfiles.size();i++)
        {
            System.out.println(queryfiles.get(i));
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
        System.out.println("the total query member: "+queryfiles.size());
        System.out.println("the total query diff: "+count);
    }




    public static List<String> getDirectoryList(String dir){
        List<String>  files=new ArrayList<>();
        File f = new File(dir);
        if(!f.isDirectory()){
            System.out.println("not a directory!");
            files.add(f.getPath());
        }
        else{
            File[] t = f.listFiles();
            for(int i=0;i<t.length;i++){
                  if(!t[i].isDirectory()){
                   files.add(t[i].getPath());
                }
            }
        }
        return files;

    }

}
