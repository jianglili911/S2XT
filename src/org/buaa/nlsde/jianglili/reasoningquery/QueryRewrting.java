package org.buaa.nlsde.jianglili.reasoningquery;

import org.apache.jena.sparql.algebra.Op;
import org.buaa.nlsde.jianglili.reasoningquery.assertionReplace.AlgebraTransformer;
import org.buaa.nlsde.jianglili.reasoningquery.conceptExtract.Concept;
import org.buaa.nlsde.jianglili.reasoningquery.conceptExtract.ConceptUtil;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jianglili on 2016/3/23.
 */
public class QueryRewrting {

    //
    public static List<Op> exe( List<Op> ops, String schemafile, int tag) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {
        OWLOntology ontology= ConceptUtil.getSchema(schemafile);
        //get all concrete and (class or properties)
        Concept concept=new Concept();
        // prepare all the reasoning and schema gaph work
        concept.setOntology(ontology);
        //Transform each Op (from old op to new op)
        List<Op> newOpList=new ArrayList<>();
        for(Op op:ops) {
           System.out.println(op);
            newOpList.add(transform(op, concept));
        }
        return newOpList;

    }
    public static Concept initSchema( String schemafile, int tag) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {
        OWLOntology ontology= ConceptUtil.getSchema(schemafile);
        //get all concrete and (class or properties)
        Concept concept=new Concept();
        // prepare all the reasoning and schema gaph work
        concept.setOntology(ontology);
        //Transform each Op (from old op to new op)
      return concept;


    }
    public static Op transform( Op op, Concept concept) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {
     //  return new AlgebraTransformer(concept).AxiomTransform(op);
        return new AlgebraTransformer(concept,true).BothTransform(op);
    //   return new AlgebraTransformer(concept).transform(op);
    }
    public static Op transformDBpedia( Op op, Concept concept,Boolean extractFlag) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {
        //  return new AlgebraTransformer(concept).AxiomTransform(op);
        return new AlgebraTransformer(concept,extractFlag).transform(op);
        //   return new AlgebraTransformer(concept).transform(op);
    }

}
