package org.buaa.nlsde.jianglili.utils.unused;

import com.clarkparsia.owlapiv3.OWL;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_URI;
import org.apache.jena.graph.Triple;
import org.apache.jena.ontology.OntClass;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.ontology.OntModelSpec;
import org.apache.jena.ontology.OntProperty;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by jianglili on 2016/1/3.
 */
public class QueryBranch2 {
    public static void main(String[] args) throws FileNotFoundException {
//        Model model = RDFDataMgr.loadModel("e://ontology.owl") ;
//        model.write(new PrintStream(new File("e://ontology.ttl")),"TTL");
//        OntModel om = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM_RULE_INF, model);
        Model model = RDFDataMgr.loadModel("e://ontology.ttl") ;
        OntModel om = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM, model);


    }
    public static void main2(String[] args) throws FileNotFoundException, OWLOntologyCreationException {
        Model model = RDFDataMgr.loadModel("e://people.owl") ;
        model.write(new PrintStream(new File("e://people.ttl")),"TTL");
        String  query1="e://people1.rq";    //prop parent
        String  query2="e://people2.rq";    //prop father
        String  query3="e://people3.rq";    //prop mother
        printQueryResult(query1,model);
        printQueryResult(query2,model);
        printQueryResult(query3, model);
        //等价属性
        String  file="file:e://people.owl";
        queryPropEntailment(query1, model,file);
    }

     //直接查询
    public static void  printQueryResult(String queryfile, Model model){

        Query query= QueryFactory.read(queryfile);
        QueryExecution qexec = QueryExecutionFactory.create(query, model);
        ResultSet rs = qexec.execSelect();
        ResultSetFormatter.out(System.out, rs, query);
    }

    //获取显式属性的等价属性
    public static ExtendedIterator<? extends OntProperty> getEquivalentProperties(OntModel m, String cUri){
        OntProperty  p= m.getOntProperty(cUri);
        if(p==null) return null;
        return p.listEquivalentProperties();
    }
    //获取隐式属性的等价属性  boolean = true
    public static ExtendedIterator<? extends OntProperty> getEquivalentProperties(OntModel m, String cUri,boolean b){
        OntProperty  p= m.getOntProperty(cUri);
        if(p==null) return null;
        return p.listEquivalentProperties();
    }
    //获取显式类的子类
    public static ExtendedIterator<? extends OntProperty> getSubProperties(OntModel m, String cUri){
        OntProperty  p= m.getOntProperty(cUri);
        if(p==null) return null;
        return p.listSubProperties();
    }
    //获取隐式类的子类  boolean = true
    public static ExtendedIterator<? extends OntProperty> getSubProperties(OntModel m, String cUri,boolean b){
        OntProperty  p= m.getOntProperty(cUri);
        if(p==null) return null;
        return p.listSubProperties(true);
    }
    //获取显式类的等价类
    public static ExtendedIterator<OntClass> getEquivalentClasses(OntModel m, String cUri){
        OntClass c = m.getOntClass(cUri);
        if(c==null) return null;
        return c.listEquivalentClasses();
    }
    //获取隐式类的等价类  boolean = true
    public static ExtendedIterator<OntClass> getEquivalentClasses(OntModel m, String cUri,boolean b){
        OntClass c = m.getOntClass(cUri);
        if(c==null) return null;
        return c.listEquivalentClasses();
    }
    //获取显式类的子价类
    public static ExtendedIterator<OntClass> geSubClasses(OntModel m, String cUri){
        OntClass c = m.getOntClass(cUri);
        if(c==null) return null;
        return c.listSubClasses();
    }
    //获取隐式类的子类  boolean = true
    public static ExtendedIterator<OntClass> getSubClasses(OntModel m, String cUri,boolean b){
        OntClass c = m.getOntClass(cUri);
        if(c==null) return null;
        return c.listSubClasses(true);
    }

    //怎样获取query中的概念
    //怎样获取中schema中的概念
    public static void queryPropEntailment(String queryfile, Model model,String file) throws OWLOntologyCreationException {
        Query query= QueryFactory.read(queryfile);
        final Element pattern = query.getQueryPattern();
        final ElementGroup elementGroup = (ElementGroup) pattern;
        final List<Element> elements = elementGroup.getElements();
        final Element first = elements.get(0);
        List<Triple> triples = new ArrayList<Triple>();
        for (TriplePath path : ((ElementPathBlock) first).getPattern()) {
            if (!path.isTriple()) {
                System.out.print("Path expressions are not supported yet.");
            }
            triples.add(path.asTriple());
        }
        //假如只有一个where { :liyan ns0:has_parent ?p }三元组
        Node predicate=(Node_URI)triples.get(0).getPredicate();
//        Node subject=triples.get(0).getSubject();
//        Node object=triples.get(0).getObject();
//        OWLObjectProperty p=new OWLObjectPropertyImpl(IRI.create(predicate.toString()));
        OWLObjectProperty p= OWL.ObjectProperty(predicate.toString());

        // Create an OWLAPI manager that allows to load an ontology
        OWLOntologyManager manager = OWLManager.createOWLOntologyManager();

        // Load the ontology file into an OWL ontology object
        OWLOntology ontology = manager.loadOntology(IRI.create(file));
        // ArrayList<Var> projectVars=(ArrayList<Var>)query.getProjectVars();

        //
        Set<OWLSubClassOfAxiom> axioms=ontology.getAxioms(AxiomType.SUBCLASS_OF);
        Set<OWLEquivalentClassesAxiom>  equivalentClassesAxioms=ontology.getAxioms(AxiomType.EQUIVALENT_CLASSES);

        //TBox
        Set<OWLAxiom>  tboxs=ontology.getTBoxAxioms(true);
        Set<OWLAxiom>  ftboxs=ontology.getTBoxAxioms(false);

        //RBox
        Set<OWLAxiom>  rboxs=ontology.getRBoxAxioms(true);
        Set<OWLAxiom>  frboxs=ontology.getRBoxAxioms(false);


        Set<OWLSubObjectPropertyOfAxiom> subprops= ontology.getObjectSubPropertyAxiomsForSubProperty(p);
        Set<OWLSubObjectPropertyOfAxiom> subprops2=ontology.getObjectSubPropertyAxiomsForSuperProperty(p);



        QueryExecution qexec = QueryExecutionFactory.create(query, model);
        ResultSet rs = qexec.execSelect();
        ResultSetFormatter.out(System.out, rs, query);

    }
}
