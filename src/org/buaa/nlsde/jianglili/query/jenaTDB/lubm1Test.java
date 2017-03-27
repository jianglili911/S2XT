/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.buaa.nlsde.jianglili.query.jenaTDB;

import de.tf.uni.freiburg.sparkrdf.constants.Const;
import de.tf.uni.freiburg.sparkrdf.run.QueryExecutor;
import org.apache.jena.ext.com.google.common.collect.Range;
import org.apache.jena.query.*;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.QueryEngineBase;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIteratorCheck;
import org.apache.jena.sparql.engine.iterator.QueryIteratorCloseable;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.tdb.TDBFactory ;
import org.apache.jena.tdb.solver.QueryEngineTDB;
import org.apache.jena.tdb.solver.QueryIterTDB;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.buaa.nlsde.jianglili.reasoningquery.QueryRewrting;
import org.buaa.nlsde.jianglili.reasoningquery.conceptExtract.Concept;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TreeMap;

/** Example of a READ transaction. */
public class lubm1Test
{
    public  static Logger log = Logger.getLogger(lubm1Test.class);
    public static Map<String, Long> operationDuration = new TreeMap<>();
    public static void main(String... argv) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {
        String datafile="E://benchmarks//LUBM//tdb//lubm1";
     //   String queryfile="E:\\benchmarks\\LUBM\\query28-ub\\query1.rq";
        String queryDir="E:\\benchmarks\\LUBM\\query28-ub";
        Const.timeFilePath_$eq("E:\\benchmarks\\LUBM\\count\\tdbcount\\lubm1.txt");
       // query(queryfile,datafile);
        queryFileList(queryDir,datafile);



    }


    public static void queryFileList(String  queryfile,String datafile) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {
         int[]  nums=new int[21];
        for(int i=1;i<=18;i++)
            nums[i-1]=i;
        nums[18]=20; nums[19]=22; nums[20]=28;
        //load the schema
        Concept concept= QueryRewrting.initSchema("file:"+ "E://benchmarks//LUBM//univ-benchQL-ub.owl",0);

        queryfile(queryfile + "\\query1.rq", datafile,concept,1);
        queryfile(queryfile + "\\query1.rq", datafile,concept,2);

        for(int i=0;i<nums.length;i++) {
            queryfile(queryfile + "\\query" + nums[i] + ".rq", datafile,concept,1);
            queryfile(queryfile + "\\query" + nums[i] + ".rq", datafile,concept,2);
        }
    }
    public static void queryfile(String  queryfile,String datafile,Concept concept,Integer qc) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {
        System.out.println("query: "+queryfile+"file: "+datafile);
        long startRewrite = System.currentTimeMillis();
        Query query = QueryFactory.read(queryfile);
        Op opRoot = Algebra.compile(query) ;
            // rewrite the op
        Op opRootRewrite= QueryRewrting.transform(opRoot, concept);
        //end rewrite
        Dataset dataset = TDBFactory.createDataset(datafile) ;
        dataset.begin(ReadWrite.READ) ;
//        QueryExecution qexec = QueryExecutionFactory.create(query, dataset) ;
//        qexec.execSelect();
        // Execute it.
        QueryIterator qIter = Algebra.exec(opRootRewrite, dataset) ;
        int results = 0;
        QueryIteratorCloseable queryIteratorCloseable;
        for ( ; qIter.hasNext() ; )
        {
            Binding b = qIter.nextBinding() ;
            results++;
         //   System.out.println(b) ;
        }
        qIter.close() ;
        dataset.end() ;
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
                fbw.write("["+sfd.format(System.currentTimeMillis())+"]"+queryFile + ";");
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
    public static void query(String  queryfile,String datafile) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {
        Query query = QueryFactory.read(queryfile);
        Op opRoot = Algebra.compile(query) ;
        //load the schema
        Concept concept= QueryRewrting.initSchema("file:"+ "E://benchmarks//LUBM//univ-benchQL.owl",0);
        // rewrite the op
        Op opRootRewrite= QueryRewrting.transform(opRoot, concept);
        //end rewrite
        Dataset dataset = TDBFactory.createDataset(datafile) ;
        dataset.begin(ReadWrite.READ) ;
        QueryExecution qexec = QueryExecutionFactory.create(query, dataset) ;
        // Execute it.
        QueryIterator qIter = Algebra.exec(opRootRewrite, dataset) ;
        int results = 0;
        for ( ; qIter.hasNext() ; )
        {
            Binding b = qIter.nextBinding() ;
            results++;
            System.out.println(b) ;
        }
        qIter.close() ;
        System.out.println("# solution mappings: "+results);
        dataset.end() ;
    }


}

