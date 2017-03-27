package nl.vu.jena.sparql.engine.iterator;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Literal;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBase;
import org.apache.jena.sparql.engine.iterator.QueryIter1;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.apache.jena.sparql.util.Utils;
import nl.vu.jena.graph.FilteredTriple;
import nl.vu.jena.graph.HBaseGraph;
import nl.vu.jena.sparql.engine.binding.BindingMaterializer;
import org.apache.jena.atlas.io.IndentedWriter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class QueryIterBlockTriples extends QueryIter1
{
    //TODO private BasicPattern pattern ; check if can be removed;
	//TODO only needed for printing
    private Graph graph ;
    private QueryIterator output ;
    private BindingMaterializer bindingMaterializer;
   
    
    public static QueryIterator create(QueryIterator input,
                                       BasicPattern pattern , 
                                       ExecutionContext execContext)
    {
        return new QueryIterBlockTriples(input, pattern, execContext) ;
    }
    
    private QueryIterBlockTriples(QueryIterator input,
                                    BasicPattern pattern , 
                                    ExecutionContext execContext)
    {
        super(input, execContext) ;
        //this.pattern = pattern ;
        graph = execContext.getActiveGraph() ;
		if (graph instanceof HBaseGraph) {
			output = buildChainOfIdBasedTriples(pattern, (HBaseGraph)graph, execContext);	        
			bindingMaterializer = new BindingMaterializer(graph);
		}
		else {
			// Create a chain of triple iterators.
			QueryIterator chain = getInput();
			for (Triple triple : pattern)
				chain = new QueryIterTriplePattern(chain, triple, execContext);
			output = chain;
		}
       
        
    }

	private QueryIterator buildChainOfIdBasedTriples(BasicPattern pattern, HBaseGraph graph, ExecutionContext execContext) {
		Map<Node, Node_Literal> node2NodeIdMap = new HashMap<Node, Node_Literal>();
		for (Triple triple : pattern) {
			addNodeToMap(node2NodeIdMap, triple.getSubject());
			addNodeToMap(node2NodeIdMap, triple.getPredicate());
			addNodeToMap(node2NodeIdMap, triple.getObject());
		}
		
		try {
			graph.mapMaterializedNodesToNodeIds(node2NodeIdMap);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Create a chain of triple iterators.
		QueryIterator chain = getInput() ;
		for (Triple triple : pattern){
			Node newSubject = mapNode(node2NodeIdMap, triple.getSubject());
			Node newPredicate = mapNode(node2NodeIdMap, triple.getPredicate());
			Node newObject = mapNode(node2NodeIdMap, triple.getObject());
			Triple idBasedTriple;
			//if one of the concrete elements did not have a matching Id, we don't create a triple for them
			if (newSubject!=null && newPredicate!=null && newObject!=null) { 
				if (triple instanceof FilteredTriple) {
					idBasedTriple = new FilteredTriple(newSubject, newPredicate, newObject,
							((FilteredTriple) triple).getSimpleFilter());
				} else {
					idBasedTriple = new Triple(newSubject, newPredicate, newObject);
				}

				chain = new QueryIterTriplePattern(chain, idBasedTriple, execContext);
			}
			else {
				//this subquery has missing elements so it won't return any results
				//we still insert it with dummy nodes so that we get empty bindings when we resolve it
				Triple dummyTriple = new Triple(Node.NULL, Node.NULL, Node.NULL);
				chain = new QueryIterTriplePattern(chain, dummyTriple, execContext);
			}
			
		}
		return chain;
	}

	private Node mapNode(Map<Node, Node_Literal> node2NodeIdMap, Node oldNode) {
		return oldNode.isConcrete() ? node2NodeIdMap.get(oldNode) : oldNode;
	}

	private void addNodeToMap(Map<Node, Node_Literal> node2NodeIdMap, Node node) {
		if (node.isConcrete()){
			node2NodeIdMap.put(node, null);
		}
	}

    @Override
    protected boolean hasNextBinding()
    {
        return output.hasNext() ;
    }

    @Override
    protected Binding moveToNextBinding()
    {
        Binding binding = output.nextBinding() ;
        if (binding instanceof BindingBase && graph instanceof HBaseGraph){
        	try {
				binding = bindingMaterializer.materialize(binding);
			} catch (IOException e) {
				return null;
			}
        }
        
        return binding;
    }

    @Override
    protected void closeSubIterator()
    {
        if ( output != null ){
            output.close() ;
            if (graph instanceof HBaseGraph){
            	bindingMaterializer.close();
            }
        }
        output = null ;
    }
    
    @Override
    protected void requestSubCancel()
    {
        if ( output != null )
            output.cancel();
    }


    protected void details(IndentedWriter out, SerializationContext sCxt)
    {
        out.print(Utils.className(this)) ;
        out.println() ;
        out.incIndent() ;
        //FmtUtils.formatPattern(out, pattern, sCxt) ;
        out.decIndent() ;
    }
}
