package org.buaa.nlsde.jianglili.query.JenaMemory;

import de.tf.uni.freiburg.sparkrdf.constants.Const;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sdb.SDBFactory;
import org.apache.jena.sdb.Store;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.buaa.nlsde.jianglili.query.jenaSDB.lubm1Test;
import org.buaa.nlsde.jianglili.reasoningquery.QueryRewrting;
import org.buaa.nlsde.jianglili.reasoningquery.conceptExtract.Concept;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by jianglili on 2017/2/3.
 */
public class lubm1 {

    public  static Logger log = Logger.getLogger(lubm1Test.class);
    public static Map<String, Long> operationDuration = new TreeMap<>();
    public static void main(String... argv) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {

        String datafile="file:///E:\\benchmarks\\LUBM\\datasets\\nt\\Universities_1_new.nt";
        String queryDir="E:\\benchmarks\\LUBM\\query28-ub";
        Const.timeFilePath_$eq("E:\\benchmarks\\LUBM\\count\\memory\\lubm1.txt");
        queryFileList(queryDir,datafile);

    }


    public static void queryFileList(String  queryDir,String datafile) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {
        int[]  nums=new int[21];
        for(int i=1;i<=18;i++)
            nums[i-1]=i;
        nums[18]=20; nums[19]=22; nums[20]=28;
        //load the schema
        Concept concept= QueryRewrting.initSchema("file:"+ "E://benchmarks//LUBM//univ-benchQL-ub.owl",0);
        Model model = RDFDataMgr.loadModel(datafile) ;
        Dataset dataset =DatasetFactory.create(model);


        for(int i=0;i<nums.length;i++) {
            queryfile(queryDir + "\\query" + nums[i] + ".rq", dataset,concept,1);
            queryfile(queryDir + "\\query" + nums[i] + ".rq", dataset,concept,2);
        }
    }
    public static void queryfile(String queryfile,  Dataset dataset , Concept concept,Integer qc) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {

        System.out.println("query: "+queryfile+"datastore: "+dataset);
        long startRewrite = System.currentTimeMillis();
        Query query = QueryFactory.read(queryfile);
        Op opRoot = Algebra.compile(query) ;
        // rewrite the op
        Op opRootRewrite= QueryRewrting.transform(opRoot, concept);
        //end rewrite
      // Execute it.
        QueryIterator qIter = Algebra.exec(opRootRewrite, dataset) ;
        int results = 0;
        for ( ; qIter.hasNext() ; )
        {
            Binding b = qIter.nextBinding() ;
            results++;
            //   System.out.println(b) ;
        }
        qIter.close() ;

        long endRewrite = System.currentTimeMillis()-startRewrite;
        operationDuration.put("time",endRewrite);
        operationDuration.put("resultCount",Long.valueOf(results));
        System.out.println("# solution mappings: "+results +"  time: "+results);
        printCount(queryfile,results,qc);
    }

    public static void printCount(String queryFile,Integer resCount,Integer qc){
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


    public static void query(String queryString, Store store, Integer qc)
    {
        Query query = QueryFactory.create(queryString) ;
        Dataset ds = SDBFactory.connectDataset(store) ;
        try(QueryExecution qe = QueryExecutionFactory.create(query, ds)) {
            ResultSet rs = qe.execSelect() ;
            ResultSetFormatter.out(rs) ;
        }
        store.close() ;
    }
}
