package test.query;

import de.tf.uni.freiburg.sparkrdf.constants.Const;
import de.tf.uni.freiburg.sparkrdf.run.QueryExecutor;
import de.tf.uni.freiburg.sparkrdf.sparql.SparkFacade;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.buaa.nlsde.jianglili.reasoningquery.QueryRewrting;
import org.buaa.nlsde.jianglili.reasoningquery.conceptExtract.Concept;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;

import java.io.FileNotFoundException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by jianglili on 2017/2/21.
 */
public class DataLoader {

    public static  void loadData() throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {
        Logger log = Logger.getLogger(DataLoader.class);
        Map<String, Long> operationDuration = new TreeMap<>();
        long resCount = 0;
        SparkFacade.createSparkContext();
        // Load the graph
        log.log(Level.INFO, "Started Graph loading");
        long startLoading = System.currentTimeMillis();
        SparkFacade.loadGraph();
        long endLoading = System.currentTimeMillis() - startLoading;
        log.log(Level.INFO, "Finished Graph Loading in " + endLoading + " ms");
        operationDuration.put("GraphLoading", endLoading);

        //load the schema
        log.log(Level.INFO, "Starting schema Loading" );
        long startLSchema = System.currentTimeMillis();
        Concept concept= QueryRewrting.initSchema("file:"+ Const.schema(),0);
        long endSchema = System.currentTimeMillis() - startLoading;
        log.log(Level.INFO, "Finished schema Loading in " + endSchema + " ms");
        operationDuration.put("LoadingSchema", endSchema);
    }
}
