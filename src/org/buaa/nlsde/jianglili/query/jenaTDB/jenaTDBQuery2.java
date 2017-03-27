package org.buaa.nlsde.jianglili.query.jenaTDB;

import org.apache.jena.assembler.Assembler;
import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.shared.JenaException;
import org.apache.jena.sparql.core.assembler.DatasetAssemblerVocab;
import org.apache.jena.sparql.util.TypeNotUniqueException;
import org.apache.jena.sparql.util.graph.GraphUtils;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.tdb.assembler.VocabTDB;
import org.apache.jena.update.*;

/**
 * Created by jianglili on 2017/1/21.
 */
public class jenaTDBQuery2 {
    public static void main2(String[] args) {
        // Make a TDB-backed dataset
        String directory = "D:\\Program Files (x86)\\Apache Software Foundation\\tdb-data" ;
        Dataset dataset = TDBFactory.createDataset(directory) ;

        dataset.begin(ReadWrite.READ) ;
        // Get model inside the transaction
        Model model = dataset.getDefaultModel() ;
        dataset.end() ;

        dataset.begin(ReadWrite.WRITE) ;
        model = dataset.getDefaultModel() ;
        dataset.end() ;
    }

    public static void main(String[] args) {
        // Assembler way: Make a TDB-back Jena model in the named directory.
        // This way, you can change the model being used without changing the code.
        // The assembler file is a configuration file.
        // The same assembler description will work in Fuseki.
        String assemblerFile = "D:\\Program Files (x86)\\Apache Software Foundation\\Store\\tdb-assembler.ttl" ;
        Dataset dataset = TDBFactory.assembleDataset(assemblerFile) ;

        dataset.begin(ReadWrite.READ) ;
        // Get model inside the transaction
        Model model = dataset.getDefaultModel() ;
        dataset.end() ;
    }

    public static void getDefaultData(String... argv)
    {
        // Direct way: Make a TDB-back Jena model in the named directory.
        String directory = "D:\\Program Files (x86)\\Apache Software Foundation\\tdb-data" ;
        Dataset ds = TDBFactory.createDataset(directory) ;
        Model model = ds.getDefaultModel() ;

        // ... do work ...

        // Close the dataset.
        ds.close();

    }

    /**
     * Using an assembler description (see wiki for details of the assembler format for TDB)
     * This way, you can change the model being used without changing the code.
     * The assembler file is a configuration file.
     * The same assembler description will work as part of a Joseki configuration file.
     */
    public static void assembler(String... argv)
    {
        String assemblerFile = "Store/tdb-assembler.ttl" ;

        Dataset ds = TDBFactory.assembleDataset(assemblerFile) ;

        // ... do work ...

        ds.close() ;
    }

    /**
     * Examples of finding an assembler for a TDB model in a larger collection
     * of descriptions in a single file.
     */
    public static void assembler2(String... argv)
    {
        String assemblerFile = "Store/tdb-assembler.ttl" ;

        // Find a particular description in the file where there are several:
        Model spec = RDFDataMgr.loadModel(assemblerFile) ;

        // Find the right starting point for the description in some way.
        Resource root = null ;

        if ( false )
            // If you know the Resource URI:
            root = spec.createResource("http://example/myChoiceOfURI" );
        else
        {
            // Alternatively, look for the a single resource of the right type.
            try {
                // Find the required description - the file can contain descriptions of many different types.
                root = GraphUtils.findRootByType(spec, VocabTDB.tDatasetTDB) ;
                if ( root == null )
                    throw new JenaException("Failed to find a suitable root") ;
            } catch (TypeNotUniqueException ex)
            { throw new JenaException("Multiple types for: "+ DatasetAssemblerVocab.tDataset) ; }
        }

        Dataset ds = (Dataset) Assembler.general.open(root) ;
    }

    /** Example of creating a TDB-backed model.
     *  The preferred way is to create a dataset then get the mode required from the dataset.
     *  The dataset can be used for SPARQL query and update
     *  but the Model (or Graph) can also be used.
     *
     *  All the Jena APIs work on the model.
     *
     *  Calling TDBFactory is the only place TDB-specific code is needed.
     */
    public static void query1(String... argv)
    {
        // Direct way: Make a TDB-back Jena model in the named directory.
        String directory = "D:\\Program Files (x86)\\Apache Software Foundation\\tdb-data" ;
        Dataset dataset = TDBFactory.createDataset(directory) ;

        // Potentially expensive query.
        String sparqlQueryString = "SELECT (count(*) AS ?count) { ?s ?p ?o }" ;
        // See http://incubator.apache.org/jena/documentation/query/app_api.html

        Query query = QueryFactory.create(sparqlQueryString) ;
        QueryExecution qexec = QueryExecutionFactory.create(query, dataset) ;
        ResultSet results = qexec.execSelect() ;
        ResultSetFormatter.out(results) ;
        qexec.close() ;

        dataset.close();
    }

    /** Example of single threaded use of TDB workign with the Jena RDF API */

    public static void main5(String[] args) {
        //   queryDiretory(args);
        queryDiretoryRead(args);
        //    queryAssembler(args);
    }

    public static void queryDiretoryRead(String[] args) {
        // Make a TDB-backed dataset
        String directory = "D:\\Program Files\\apache-jena-3.0.0\\Databases\\dbpedia" ;
        Dataset dataset = TDBFactory.createDataset(directory) ;

        dataset.begin(ReadWrite.READ) ;
        // Get model inside the transaction
        Model model = dataset.getDefaultModel() ;

        String qs1 = "SELECT * {?s ?p ?o} LIMIT 10" ;

        try(QueryExecution qExec = QueryExecutionFactory.create(qs1, dataset)) {
            ResultSet rs = qExec.execSelect() ;
            ResultSetFormatter.out(rs) ;
        }
//
//        String qs2 = "SELECT * {?s ?p ?o} OFFSET 10 LIMIT 10" ;
//        try(QueryExecution qExec = QueryExecutionFactory.create(qs2, dataset)) {
//            ResultSet   rs = qExec.execSelect() ;
//            ResultSetFormatter.out(rs) ;
//        }
        dataset.end() ;


    }
    public static void queryDiretoryWrite(String[] args){
        // Make a TDB-backed dataset
        String directory = "MyDatabases/Dataset1" ;
        Dataset dataset = TDBFactory.createDataset(directory) ;

        dataset.begin(ReadWrite.WRITE) ;
        Model model = dataset.getDefaultModel() ;
        try {

            //    model.add( );

            // A SPARQL query will see the new statement added.
            try (QueryExecution qExec = QueryExecutionFactory.create(
                    "SELECT (count(*) AS ?count) { ?s ?p ?o} LIMIT 10",
                    dataset)) {
                ResultSet rs = qExec.execSelect() ;
                ResultSetFormatter.out(rs) ;
            }
            // ... perform a SPARQL Update
            GraphStore graphStore = GraphStoreFactory.create(dataset) ;
            String sparqlUpdateString = StrUtils.strjoinNL(
                    "PREFIX . <http://example/>",
                    "INSERT { :s :p ?now } WHERE { BIND(now() AS ?now) }"
            ) ;
            UpdateRequest request = UpdateFactory.create(sparqlUpdateString) ;
            UpdateProcessor proc = UpdateExecutionFactory.create(request, graphStore) ;
            proc.execute() ;

            // Finally, commit the transaction.
            dataset.commit() ;
            // Or call .abort()
        } finally {
            dataset.end() ;
        }
        dataset.end() ;

    }

    public static void queryAssembler(String[] args) {
        // Assembler way: Make a TDB-back Jena model in the named directory.
        // This way, you can change the model being used without changing the code.
        // The assembler file is a configuration file.
        // The same assembler description will work in Fuseki.
        String assemblerFile = "Store/tdb-assembler.ttl" ;
        Dataset dataset = TDBFactory.assembleDataset(assemblerFile) ;

        dataset.begin(ReadWrite.READ) ;
        // Get model inside the transaction
        Model model = dataset.getDefaultModel() ;
        dataset.end() ;
    }
}
