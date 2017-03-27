package org.buaa.nlsde.jianglili.query.spark


import java.io.FileNotFoundException
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import de.tf.uni.freiburg.sparkrdf.constants.Const
import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.IntermediateResultsModel
import de.tf.uni.freiburg.sparkrdf.parser.query.op.SparkOp
import de.tf.uni.freiburg.sparkrdf.parser.query.{AlgebraTranslator, AlgebraWalker}
import de.tf.uni.freiburg.sparkrdf.sparql.SparkFacade
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping
import org.apache.jena.query.{Query, QueryFactory}
import org.apache.jena.shared.PrefixMapping
import org.apache.jena.sparql.algebra.{Algebra, Op}
import org.buaa.nlsde.jianglili.reasoningquery.QueryRewrting
import org.buaa.nlsde.jianglili.reasoningquery.conceptExtract.Concept
import org.mortbay.jetty.{HttpStatus, Request, Server}
import org.mortbay.jetty.handler.{AbstractHandler}
import org.semanticweb.owlapi.model.{OWLOntologyCreationException, OWLOntologyStorageException}
import org.json.JSONObject

/**
  * E:/benchmarks/LUBM/datasets/nt1 3g  1 E://benchmarks//LUBM//univ-benchQL-ub.owl 9998
  * Created by jianglili on 2017/2/27.
  */
object S2XTServer extends AbstractHandler {

   //use the main S2XT server
  def main(args: Array[String]): Unit = {

    init(args(0),args(1),Integer.parseInt(args(2))==1,args(3))
    val server = new Server(Integer.parseInt(args(4)))
    println("S2XT server start on: " + args(4))
    server.setHandler(this)
    server.start()

  }

   var concept:Concept=null
  var datapre:Int=1
  /**
    * 处理请求 返回响应
    *
    * @param target
    * @param request
    * @param response
    * @param dispatch
    */
  override def handle(target: String,
                      request: HttpServletRequest,
                      response: HttpServletResponse,
                      dispatch: Int): Unit = {

    //get the url and parameters
    val url = request.getRequestURI
    response.setHeader("Access-Control-Allow-Origin", "*");
    response.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
    response.setHeader("Access-Control-Allow-Headers", "X-Requested-With,Content-Type,Accept,Origin");
    // get the query parameter
    var query:String=request.getParameter("q")
    val data=request.getParameter("data")
    if(query==null||data==null)
    {
      response.setStatus(HttpStatus.ORDINAL_404_Not_Found);
      request.asInstanceOf[Request].setHandled(true)
    }
    else {
      query = query.replaceAll("&lt;", "<").replaceAll("&gt;", ">")
      url.substring(url.indexOf("/") + 1, url.length) match {

        case "query/s2xquery" => {

          println("s2xquery request：" + url)
          println("data parameter: " + data)
          println("query parameter: " + query)
          //get  the jObject query result
          //  val query: Query = QueryFactory.read("E:/benchmarks/LUBM/query28/query1.rq")
          val jObject: JSONObject = new JSONObject()
          val start: Long = System.currentTimeMillis
          val solutionMappings: java.util.List[SolutionMapping] = runSPARQLQuery(query)
          val timespend: Long = System.currentTimeMillis - start
          jObject.put("time", timespend)
          System.out.println("time:" + timespend)
          jObject.put("result", "ok")
          jObject.put("queryResult", solutionMappings)
          response.setContentType("application/json;charset=utf-8")
          println(jObject)
          response.getWriter().println(jObject)
          response.setStatus(HttpStatus.ORDINAL_200_OK)
          request.asInstanceOf[Request].setHandled(true)
          response.getWriter.close()
        }
        case "query/s2xqueryrewrite" => {

          println("s2xqueryrewrite request：" + url)
          println("data parameter: " + data)
          println("query parameter: " + query)
          // get the jObject query results
          val jObject: JSONObject = new JSONObject()
          val start: Long = System.currentTimeMillis
          val solutionMappings: java.util.List[SolutionMapping] = runSPARQLQueryRewrite(query.toString, concept)
          val timespend: Long = System.currentTimeMillis - start
          jObject.put("time", timespend)
          jObject.put("result", "ok")
          jObject.put("queryResult", solutionMappings)
          response.setContentType("application/json;charset=utf-8")
          println(jObject)
          response.getWriter().println(jObject)
          response.setStatus(HttpStatus.ORDINAL_200_OK)
          request.asInstanceOf[Request].setHandled(true)
          response.getWriter.close()

        }
        case _ => {
          response.setStatus(HttpStatus.ORDINAL_404_Not_Found);
          request.asInstanceOf[Request].setHandled(true)
        }
      }
    }

  }



  def init(data: String, executorMem: String, local: Boolean,schema:String) {
    Const.inputFile_$eq(data)
    Const.executorMem_$eq(executorMem)
    Const.locale_$eq(local)
    Const.schema_$eq(schema)
    concept= QueryRewrting.initSchema("file:" + Const.schema, 0)
    SparkFacade.createSparkContext
    SparkFacade.loadGraph
  }

  @throws[OWLOntologyCreationException]
  @throws[OWLOntologyStorageException]
  @throws[FileNotFoundException]
  def runSPARQLQuery(queryString: String): java.util.List[SolutionMapping] = {
      this.synchronized {
        IntermediateResultsModel.getInstance.clearResults()
        val query: Query = QueryFactory.create(queryString)
        val prefixes: PrefixMapping = query.getPrefixMapping
        val opRoot: Op = Algebra.compile(query)
        val trans: AlgebraTranslator = new AlgebraTranslator(prefixes)
        opRoot.visit(new AlgebraWalker(trans))
        val q: java.util.Queue[SparkOp] = trans.getExecutionQueue
        while (!q.isEmpty) {
          {
            val actual: SparkOp = q.poll
            actual.execute
          }
        }
        return IntermediateResultsModel.getInstance.getFinalResultAsList
      }
  }

  @throws[OWLOntologyCreationException]
  @throws[OWLOntologyStorageException]
  @throws[FileNotFoundException]
  def runSPARQLQueryRewrite(queryString: String, concept: Concept): java.util.List[SolutionMapping] = {
    this.synchronized {
      IntermediateResultsModel.getInstance.clearResults()
      val query: Query = QueryFactory.create(queryString)
      val prefixes: PrefixMapping = query.getPrefixMapping
      val opRoot: Op = Algebra.compile(query)
      val opRootRewrite: Op = QueryRewrting.transform(opRoot, concept)
      val trans: AlgebraTranslator = new AlgebraTranslator(prefixes)
      opRootRewrite.visit(new AlgebraWalker(trans))
      val q: java.util.Queue[SparkOp] = trans.getExecutionQueue
      while (!q.isEmpty) {
        {
          val actual: SparkOp = q.poll
          actual.execute
        }
      }
      return IntermediateResultsModel.getInstance.getFinalResultAsList
    }
  }

  def close {
    SparkFacade.closeContext
  }
}
