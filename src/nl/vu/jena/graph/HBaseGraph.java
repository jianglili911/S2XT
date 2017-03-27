package nl.vu.jena.graph;

import org.apache.jena.graph.*;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprFunction2;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NullIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import nl.vu.datalayer.hbase.HBaseClientSolution;
import nl.vu.datalayer.hbase.id.Id;
import nl.vu.datalayer.hbase.retrieve.IHBasePrefixMatchRetrieveOpsManager;
import nl.vu.datalayer.hbase.retrieve.RowLimitPair;
import nl.vu.jena.cache.JenaCache;

import java.io.IOException;
import java.util.*;

public class HBaseGraph extends GraphBase {

	public static final byte CACHING_ON = 1;
	public static final byte CACHING_OFF = 0;
	
	private static final int CACHE_SIZE = 1000000;
	private HBaseClientSolution hbase;
	ExtendedIterator<Triple> it;
	ValueIdMapper valIdMapper;
	private byte caching;
	
	//private int totalRequests = 0;
	//private int cacheHits = 0;
	
	private Map<Triple, List<Triple>> cache = Collections.synchronizedMap(new JenaCache<Triple, List<Triple>>(CACHE_SIZE));
	
	public HBaseGraph(HBaseClientSolution hbase, byte caching) {
		super();
		this.hbase = hbase;
		valIdMapper = new ValueIdMapper(hbase);
		this.caching = caching;
	}

	@Override
	protected ExtendedIterator<Triple> graphBaseFind(Triple m) {
		//totalRequests++;
		ExtendedIterator<Triple> ret;
		List<Triple> tripleList;
		if (caching==CACHING_ON && (tripleList=cache.get(m))!=null){
			//cacheHits++;
			return WrappedIterator.createNoRemove(tripleList.iterator());
		}
		
		try {
			//this happens when there are missing bound elements
			if (m.getMatchSubject() instanceof Node_NULL &&
					m.getMatchPredicate() instanceof Node_NULL &&
					m.getMatchObject() instanceof Node_NULL){
				return NullIterator.instance();
			}
			
			Id[] quad = valIdMapper.getIdsFromTriple(m);

			// retrieve results from HBase
			ArrayList<ArrayList<Id>> results;

			if (m instanceof FilteredTriple) {
				ExprFunction2 simpleFilter = (ExprFunction2) (((FilteredTriple) m).getSimpleFilter());
				results = getFilteredResults(quad, simpleFilter);
			} else {
				results = ((IHBasePrefixMatchRetrieveOpsManager) hbase.opsManager).getResults(quad);
			}

			ArrayList<Triple> convertedTriples = new ArrayList<Triple>(results.size());
			for (ArrayList<Id> arrayList : results) {
				Triple newTriple = new Triple(NodeFactory.createUncachedLiteral(
						arrayList.get(0), null), NodeFactory.createUncachedLiteral(
						arrayList.get(1), null), NodeFactory.createUncachedLiteral(
						arrayList.get(2), null));

				convertedTriples.add(newTriple);
			}

			ret = WrappedIterator.createNoRemove(convertedTriples.iterator());
			if (caching==CACHING_ON){
				cache.put(m, convertedTriples);
			}

		} catch (Exception e) {
			e.printStackTrace();
			return NullIterator.instance();
		}
		/*if (totalRequests % 1000 == 0){
			System.out.println("Cache hits: "+(double)cacheHits/(double)totalRequests);
		}*/
		return ret;
	}
	
	public void mapNodeIdsToMaterializedNodes(Map<Node_Literal, Node> tempIdMap) throws IOException{
		valIdMapper.mapNodeIdsToMaterializedNodes(tempIdMap);
	}
	
	public void mapMaterializedNodesToNodeIds(Map<Node, Node_Literal> node2nodeIdMap) throws IOException{
		valIdMapper.mapMaterializedNodesToNodeIds(node2nodeIdMap);
	}

	private ArrayList<ArrayList<Id>> getFilteredResults(Id[] quad, ExprFunction2 simpleFilter)
			throws Exception, IOException {
		ArrayList<ArrayList<Id>> results;
		
		Expr arg1 = simpleFilter.getArg1();
		Expr arg2 = simpleFilter.getArg2();
		
		if (arg1.isConstant() && arg1.getConstant().isNumber() ||
				arg2.isConstant() && arg2.getConstant().isNumber()){
			RowLimitPair limitPair = ExprToHBaseLimitsConverter.getRowLimitPair(simpleFilter);
			results = ((IHBasePrefixMatchRetrieveOpsManager)hbase.opsManager).getResults(quad, limitPair);
		}
		/*TODO else if (simpleFilter instanceof E_Equals && 
				((arg1.isConstant()&&arg1.getConstant().isIRI()) 
				|| (arg2.isConstant()&&arg2.getConstant().isIRI()))){
			E_Equals eq = (E_Equals) simpleFilter;
			NodeValue constantNode = null;
			if (eq.getArg1().isConstant()) {
				constantNode = eq.getArg1().getConstant();
			} else if (eq.getArg2().isConstant()) {
				constantNode = eq.getArg2().getConstant();
			}
			
			Value constValue = Convert.nodeToValue(valFactory, constantNode.asNode());
			TODO have to convert equality Filters to Ids before getting here
			quad[2] = 
			results = hbase.opsManager.getResults(quad);
		}*/
		else 
			throw new RuntimeException("Unsupported simple filter: "+simpleFilter.getOpName());//TODO
		return results;
	}

	/* (non-Javadoc)
	 * @see org.apache.jena.graph.impl.GraphBase#graphBaseFind(org.apache.jena.graph.Node, org.apache.jena.graph.Node, org.apache.jena.graph.Node)
	 */
	@Override
	protected ExtendedIterator<Triple> graphBaseFind(Node s, Node p, Node o) {
		//this function also does the materialization of Ids because it's called directly from the model
		
		Map<Node_Literal, Node> toResolveIdMap = new HashMap<Node_Literal, Node>();
		
		ExtendedIterator<Triple> idTripleIterator = graphBaseFind(Triple.createMatch(s, p, o));
		List<Triple> tripleList = idTripleIterator.toList();
		
		for (Triple t : tripleList) {
			addNodeToMap(toResolveIdMap, t.getSubject());
			addNodeToMap(toResolveIdMap, t.getPredicate());
			addNodeToMap(toResolveIdMap, t.getObject());
		}
		
		try {
			mapNodeIdsToMaterializedNodes(toResolveIdMap);
		} catch (IOException e) {
			e.printStackTrace();
			return NullIterator.instance();
		}
		
		ArrayList<Triple> materializedTriples = new ArrayList<Triple>();
		for (Triple t : tripleList) {
			Triple materializedTriple = new Triple(toResolveIdMap.get(t.getSubject()),
									toResolveIdMap.get(t.getPredicate()),
									toResolveIdMap.get(t.getObject()));
			materializedTriples.add(materializedTriple);
		}
		
		return WrappedIterator.createNoRemove(materializedTriples.iterator());
	}


	private void addNodeToMap(Map<Node_Literal, Node> toResolveIdMap, Node node) {
		if (!toResolveIdMap.containsKey(node)){
			toResolveIdMap.put((Node_Literal)node, null);
		}
	}

}
