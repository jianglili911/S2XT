package org.buaa.nlsde.jianglili.utils.unused;

import org.apache.jena.ontology.OntClass;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.ontology.OntModelSpec;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.util.FileManager;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.semanticweb.owlapi.model.*;

import java.io.*;
import java.util.Scanner;

/**
 * Created by jianglili on 2016/1/3.
 */
public class QueryBranch3 {
    public static void main2(String[] args) throws FileNotFoundException, OWLOntologyCreationException {
        Model model = RDFDataMgr.loadModel("e://people.owl") ;
        model.write(new PrintStream(new File("e://people.ttl")),"TTL");
        String  query1="e://people1.rq";    //prop parent
        String  query2="e://people2.rq";    //prop father
        String  query3="e://people3.rq";    //prop mother
//        printQueryResult(query1,model);
//        printQueryResult(query2,model);
//        printQueryResult(query3, model);
//        //等价属性
//        String  file="file:e://people.owl";
//        queryPropEntailment(query1, model,file);
    }
    public static void main3 (String args[]) throws IOException,InterruptedException {

        // create an empty model
        OntModel m = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM, null);
        String inputFileName="";
        // use the class loader to find the input file
        InputStream in = FileManager.get().open(inputFileName);
        if (in == null) {
            throw new IllegalArgumentException("File: " + inputFileName + " not found");
        }

        // read the RDF/XML files
        m.read(in, "");

        Scanner user_input = new Scanner(System.in);
        String input;
        System.out.print("Enter your concept");
        input = user_input.next();

        ExtendedIterator<?> i1 = m.listClasses();
        while (i1.hasNext()) {
            OntClass oc = (OntClass) i1.next();

            if (oc.getEquivalentClass() != null) {
                input = oc.getEquivalentClass().toString();
                System.out.println("Equivalent Class name: " + oc.getEquivalentClass().getLocalName());
            }
        }
    }
}
