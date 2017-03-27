package org.buaa.nlsde.jianglili.utils.unused;

import com.clarkparsia.pellet.sparqldl.jena.SparqlDLExecutionFactory;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.mindswap.pellet.PelletOptions;
import org.mindswap.pellet.jena.PelletReasonerFactory;

/**
 * Created by jianglili on 2016/3/19.
 */
public class BnodeQueryExample {
    public static void main(String[] args) throws Exception {


        PelletOptions.TREAT_ALL_VARS_DISTINGUISHED = false;
        String ns = "http://www.w3.org/TR/2003/PR-owl-guide-20031209/food#";

        // create an empty ontology model using Pellet spec
        OntModel model = ModelFactory.createOntologyModel(PelletReasonerFactory.THE_SPEC);

        // read the file
        model.read( "file:D:/Users/git/pellet-2.3.1/examples/data/wine.owl" );

        // getOntClass is not used because of the same reason mentioned above
        // (i.e. avoid unnecessary classifications)
        Resource RedMeatCourse = model.getResource( ns + "RedMeatCourse" );
        Resource PastaWithLightCreamCourse = model.getResource( ns + "PastaWithLightCreamCourse" );

        // create two individuals Lunch and dinner that are instances of
        // PastaWithLightCreamCourse and RedMeatCourse, respectively
        model.createIndividual( ns + "MyLunch", PastaWithLightCreamCourse);
        model.createIndividual( ns + "MyDinner", RedMeatCourse);

        String queryBegin =
                "PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\r\n" +
                        "PREFIX food: <http://www.w3.org/TR/2003/PR-owl-guide-20031209/food#>\r\n" +
                        "PREFIX wine: <http://www.w3.org/TR/2003/PR-owl-guide-20031209/wine#>\r\n" +
                        "\r\n" +
                        "SELECT ?Meal ?WineColor\r\n" +
                        "WHERE {\r\n";
        String queryEnd = "}";

        // create a query that asks for the color of the wine that
        // would go with each meal course
        String queryStr1 =
                queryBegin +
                        "   ?Meal rdf:type food:MealCourse .\r\n" +
                        "   ?Meal food:hasDrink _:Wine .\r\n" +
                        "   _:Wine wine:hasColor ?WineColor" +
                        queryEnd;

        // same query as above but uses a variable instead of a bnode
        String queryStr2 =
                queryBegin +
                        "   ?Meal rdf:type food:MealCourse .\r\n" +
                        "   ?Meal food:hasDrink ?Wine .\r\n" +
                        "   ?Wine wine:hasColor ?WineColor" +
                        queryEnd;

        Query query1 = QueryFactory.create(queryStr1);
        Query query2 = QueryFactory.create( queryStr2 );

        // The following definitions from food ontology dictates that
        // PastaWithLightCreamCourse has white wine and RedMeatCourse
        // has red wine.
        //  Class(PastaWithLightCreamCourse partial
        //        restriction(hasDrink allValuesFrom(restriction(hasColor value (White)))))
        //  Class(RedMeatCourse partial
        //        restriction(hasDrink allValuesFrom(restriction(hasColor value (Red)))))
        //
        // PelletQueryEngine will successfully find the answer for the first query
        printQueryResults(
                "Running first query with PelletQueryEngine...",
                SparqlDLExecutionFactory.create(query1, model), query1 );

        // The same query (with variables instead of bnodes) will not return same answers!
        // The reason is this: In the second query we are using a variable that needs to be
        // bound to a specific wine instance. The reasoner knows that there is a wine (due
        // to the cardinality restriction in the ontology) but does not know the URI for
        // that individual. Therefore, query fails because no binding can be found.
        //
        // Note that this behavior is similar to what you get with "must-bind", "don't-bind"
        // variables in OWL-QL. In this case, variables in the query are "must-bind" variables
        // and bnodes are "don't-bind" variables.
        printQueryResults(
                "Running second query with PelletQueryEngine...",
                SparqlDLExecutionFactory.create( query2, model ), query2 );

        // When the standard QueryEngine of Jena is used we don't get the results even
        // for the first query. The reason is Jena QueryEngine evaluates the query one triple
        // at a time and thus fails when the wine individual is not found. PelletQueryEngine
        // evaluates the query as a whole and succeeds. (If the above feature, creating bnodes
        // automatically, is added to Pellet then you would get the same results here)
        printQueryResults(
                "Running first query with standard Jena QueryEngine...",
                QueryExecutionFactory.create(query1, model), query1 );
    }

    public static void printQueryResults( String header, QueryExecution qe, Query query ) throws Exception {
        System.out.println(header);

        ResultSet results = qe.execSelect();

        ResultSetFormatter.out( System.out, results, query );

        System.out.println();
    }
}
