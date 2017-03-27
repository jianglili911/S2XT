package org.buaa.nlsde.jianglili.query.hbaserdf;

import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;
import nl.vu.datalayer.hbase.HBaseClientSolution;
import nl.vu.datalayer.hbase.HBaseFactory;
import nl.vu.datalayer.hbase.Quorum;
import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;
import nl.vu.jena.graph.HBaseGraph;
import nl.vu.jena.sparql.engine.main.HBaseStageGenerator;
import nl.vu.jena.sparql.engine.optimizer.HBaseOptimize;
import nl.vu.jena.sparql.engine.optimizer.HBaseTransformFilterPlacement;
import org.apache.jena.graph.Graph;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.QueryExecutionBase;
import org.apache.jena.sparql.engine.main.StageBuilder;
import org.apache.jena.sparql.syntax.Element;
import org.buaa.nlsde.jianglili.query.hbaserdf.OpToQueryTrans.AlgebraTransformer;
import org.buaa.nlsde.jianglili.reasoningquery.QueryRewrting;
import org.buaa.nlsde.jianglili.reasoningquery.conceptExtract.Concept;
import org.codehaus.jettison.json.JSONObject;
import org.mortbay.jetty.HttpStatus;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
/**
 * Created by jianglili on 2017/3/1.
 */
public class HBaseRDFServer  extends AbstractHandler {

    //load the schema
    static Concept concept;  //load the hbase model  register the methods
    static HBaseRDFServer instance=new HBaseRDFServer();
    static Model model ;

    @Override
    public void handle(String target, HttpServletRequest request, HttpServletResponse response, int dispatch) throws IOException, ServletException {

        //get the url and parameters
        String url = request.getRequestURI();
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
        response.setHeader("Access-Control-Allow-Headers", "X-Requested-With,Content-Type,Accept,Origin");
        // get the query parameter
        String query=request.getParameter("q");
        String data=request.getParameter("data");
        if(query==null)
        {
            switch(url.substring(url.indexOf("/") + 1, url.length())){
                case "query/index":{
                    System.out.println("query/index：" + url);
                    try {
                        response.setContentType("application/xml;charset=utf-8");
                        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("les-miserables.gexf"))));
                        String line = null;
                        while((line = br.readLine())!=null) {
//                            System.out.println(line);
                            response.getWriter().write(line);
                        }
                        response.setStatus(HttpStatus.ORDINAL_200_OK);
                        ((Request)request).setHandled(true);
                        response.getWriter().close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;
                }
                default:{
                    response.setStatus(HttpStatus.ORDINAL_404_Not_Found);
                    ((Request)request).setHandled(true);
                }
            }
        }
        else {
            query=query.replaceAll("&lt;", "<").replaceAll("&gt;", ">");;
            switch(url.substring(url.indexOf("/") + 1, url.length())){
                case "query/hbasequery": {
                    System.out.println("hbasequery request：" + url);
                    System.out.println("data parameter: " + data);
                    System.out.println("query parameter: " + query);
                    if(data!=null) {
                        Quorum.confFile = "config" + data + ".properties";
                        System.out.println("data set:" + data);
                    }
                    //get  the jObject query result
                    JSONObject jObject = new JSONObject();
                    List<QuerySolution> querySolutions = null;
                    try {
                        long start = System.currentTimeMillis();
                        querySolutions = runSPARQLQuery(query,model);
                        long timespend = System.currentTimeMillis()-start;
                        jObject.put("time", timespend);
                        System.out.println("time:" + timespend);
                        jObject.put("result", "ok");
                        jObject.put("queryResult", querySolutions);
                        System.out.println(jObject);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    response.setContentType("application/json;charset=utf-8");
                    response.getWriter().println(jObject);
                    response.setStatus(HttpStatus.ORDINAL_200_OK);
                    ((Request)request).setHandled(true);
                    response.getWriter().close();
                    break;
                }
                case "query/hbasequeryrewrite" : {

                    System.out.println("hbasequeryrewrite request：" + url);
                    System.out.println("data parameter: " + data);
                    System.out.println("query parameter: " + query);
                    if(data!=null) {
                        Quorum.confFile = "config" + data + ".properties";
                        System.out.println("data set:" + data);
                    }
                    // get the jObject query results
                    JSONObject jObject = new JSONObject();
                    List<QuerySolution>   querySolutions= null;
                    try {
                        long start = System.currentTimeMillis();
                        querySolutions = runSPARQLQueryRewrite(query, model,concept);
                        long timespend = System.currentTimeMillis()-start;
                        jObject.put("time", timespend);
                        System.out.println("time:" + timespend);
                        jObject.put("result", "ok");
                        jObject.put("queryResult",  querySolutions);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    response.setContentType("application/json;charset=utf-8");
                    System.out.println(jObject);
                    response.getWriter().println(jObject);
                    response.setStatus(HttpStatus.ORDINAL_200_OK);
                    ((Request)request).setHandled(true);
                    response.getWriter().close();
                    break;
                }
                case "query/queryParse" :{
                    JSONObject jObject = new JSONObject();
                    try {
                        long start = System.currentTimeMillis();
                        String queryParseOp  = queryParse(query);
                        long timespend = System.currentTimeMillis()-start;
                        jObject.put("time", timespend);
                        System.out.println("time:" + timespend);
                        jObject.put("result", "ok");
                        jObject.put("queryParse",  queryParseOp);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    response.setContentType("application/json;charset=utf-8");
                    System.out.println(jObject);
                    response.getWriter().println(jObject);
                    response.setStatus(HttpStatus.ORDINAL_200_OK);
                    ((Request)request).setHandled(true);
                    response.getWriter().close();
                    break;
                }
                case "query/queryRewrite":{
                     JSONObject jObject = new JSONObject();
                    try {
                        long start = System.currentTimeMillis();
                        String queryRewriteOp = queryRewrite(query,concept);
                        long timespend = System.currentTimeMillis()-start;
                        jObject.put("time", timespend);
                        System.out.println("time:" + timespend);
                        jObject.put("result", "ok");
                        jObject.put("queryRewrite",  queryRewriteOp);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    response.setContentType("application/json;charset=utf-8");
                    System.out.println(jObject);
                    response.getWriter().println(jObject);
                    response.setStatus(HttpStatus.ORDINAL_200_OK);
                    ((Request)request).setHandled(true);
                    response.getWriter().close();
                    break;
                }
                default : {
                    response.setStatus(HttpStatus.ORDINAL_404_Not_Found);
                    ((Request)request).setHandled(true);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        model =init(args[0]);  //schema url
        Server server = new Server(Integer.parseInt(args[1]));  //port
        server.setHandler(instance);
        System.out.println("HBaseRDF server start on: "+args[1]+" schema file:"+args[0]);
        server.start();
    }


    public static Model init(String schema) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {

        Quorum.confFile="config" + "1" + ".properties";
        concept =QueryRewrting.initSchema("file:"+ schema,0);

        HBaseConnection con;
        try {
            con = HBaseConnection.create(HBaseConnection.NATIVE_JAVA);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        HBaseClientSolution hbaseSol = HBaseFactory.getHBaseSolution(
                "local-"+ HBPrefixMatchSchema.SCHEMA_NAME, con, null);
        Graph g = new HBaseGraph(hbaseSol, HBaseGraph.CACHING_ON);
        Model model = ModelFactory.createModelForGraph(g);
        return model;
    }
    public static  synchronized List<QuerySolution> runSPARQLQuery(String queryString, Model model) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {

        long startRewrite = System.currentTimeMillis();
        Query query = QueryFactory.create(queryString);

        //add the hbaseRDF BGP pattern into the HbaseRDF engine
        HBaseStageGenerator hBaseStageGenerator=new HBaseStageGenerator();
        StageBuilder.setGenerator(ARQ.getContext(),hBaseStageGenerator);

        ARQ.getContext().set(ARQConstants.sysOptimizerFactory, HBaseOptimize.hbaseOptimizationFactory);
        ARQ.getContext().set(ARQ.optFilterPlacement, new HBaseTransformFilterPlacement());
        QueryExecutionBase qexec = (QueryExecutionBase) QueryExecutionFactory.create(query, model);

        try {
            return executeSelect(qexec);
        } finally {
            qexec.close();
        }

    }

    public static synchronized List<QuerySolution> runSPARQLQueryRewrite(String queryString, Model model, Concept concept) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {
        Query query = QueryFactory.create(queryString);
        Op opRoot = Algebra.compile(query);
        // rewrite the op
        Op opRootRewrite= QueryRewrting.transform(opRoot, concept);
        Element element=new AlgebraTransformer().transform(opRootRewrite);
        query.setQueryPattern(element);
        // end rewrite  op to query

        //add the hbaseRDF BGP pattern into the HbaseRDF engine
        HBaseStageGenerator hBaseStageGenerator=new HBaseStageGenerator();
        StageBuilder.setGenerator(ARQ.getContext(),hBaseStageGenerator);

        ARQ.getContext().set(ARQConstants.sysOptimizerFactory, HBaseOptimize.hbaseOptimizationFactory);
        ARQ.getContext().set(ARQ.optFilterPlacement, new HBaseTransformFilterPlacement());
        QueryExecutionBase qexec = (QueryExecutionBase) QueryExecutionFactory.create(query, model);

        try {
            return executeSelect(qexec);
        } finally {
            qexec.close();
        }

    }

    private static List<QuerySolution> executeSelect(QueryExecutionBase qexec) {
        ResultSet results = qexec.execSelect();
        List<QuerySolution> listResults=new ArrayList<>();
        while (results.hasNext()){
            QuerySolution solution = results.next();
            listResults.add(solution);
        }
        return listResults;
    }

   public String queryParse(String q) throws FileNotFoundException, OWLOntologyCreationException, OWLOntologyStorageException {
       System.out.println("pre query:"+q);
        // Parse the query
        Query query = QueryFactory.create(q);
        Op opRoot = Algebra.compile(query);
        return opRoot.toString();
    }

    public String queryRewrite(String q,Concept concept) throws FileNotFoundException, OWLOntologyCreationException, OWLOntologyStorageException {
        System.out.println("pre query:"+q);
        // Parse the query
        Query query = QueryFactory.create(q);
        Op opRoot = Algebra.compile(query);
        // rewrite the op
        Op opRootRewrite= QueryRewrting.transform(opRoot, concept);
        // end the rewrite op
        return opRootRewrite.toString();
    }
    private HBaseRDFServer(){
    }
}
