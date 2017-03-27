package nl.vu.datalayer.hbase.sail;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.CloseableIteratorIteration;
import nl.vu.datalayer.hbase.HBaseClientSolution;
import org.openrdf.model.*;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.impl.TupleQueryResultImpl;
import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.NotifyingSailConnectionBase;
import org.openrdf.sail.helpers.SailBase;
import org.openrdf.sail.memory.MemoryStore;

import java.util.*;

/**
 * A connection to an {@link HBaseSail} object. This class implements methods to break down SPARQL
 * queries into statement patterns that can be used for querying HBase, to set up an in-memory
 * store for loading the quads retrieved from HBase, and finally, to run the intial SPARQL query
 * over the in-memory store and return the results.  
 * <p>
 * This class implements
 * An HBaseSailConnection is active from the moment it is created until it is closed. Care
 * should be taken to properly close HBaseSailConnections as they might block concurrent queries
 * and/or updates on the Sail while active, depending on the Sail-implementation that is being
 * used.
 * 
 * @author Anca Dumitrache
 */
public class HBaseSailConnection extends NotifyingSailConnectionBase {

	MemoryStore memStore;
	NotifyingSailConnection memStoreCon;
	HBaseClientSolution hbase;

	// Builder to write the query to bit by bit
	StringBuilder queryString = new StringBuilder();

	/**
	 * Establishes the connection to the HBase Sail, and sets up the in-memory store.
	 * 
	 * @param sailBase
	 */
	public HBaseSailConnection(SailBase sailBase) {
		super(sailBase);
		// System.out.println("SailConnection created");
		hbase = ((HBaseSail) sailBase).getHBase();

		memStore = new MemoryStore();
		try {
			memStore.initialize();
			memStoreCon = memStore.getConnection();
		} catch (SailException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	protected void addStatementInternal(Resource arg0, URI arg1, Value arg2, Resource... arg3) throws SailException {
		// TODO Auto-generated method stub
	}

	@Override
	protected void clearInternal(Resource... arg0) throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void clearNamespacesInternal() throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void closeInternal() throws SailException {
		memStoreCon.close();
	}

	@Override
	protected void commitInternal() throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected CloseableIteration<? extends Resource, SailException> getContextIDsInternal() throws SailException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected String getNamespaceInternal(String arg0) throws SailException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected CloseableIteration<? extends Namespace, SailException> getNamespacesInternal() throws SailException {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * This function sends a statement pattern to HBase for querying,
	 * then returns the results in RDF Statement format.
	 * 
	 * @param arg0
	 * @param arg1
	 * @param arg2
	 * @param arg3
	 * @param contexts
	 * @return
	 * @throws SailException
	 */
	protected CloseableIteration<? extends Statement, SailException> getStatementsInternal(Resource arg0, URI arg1,
			Value arg2, boolean arg3, Set<URI> contexts) throws SailException {
		try {

			// construct the list of contexts for this statement pattern
			ArrayList<Value> g = new ArrayList();
			if (contexts != null && contexts.size() != 0) {
				for (Resource r : contexts) {
					g.add(r);
				}
			} else {
				g.add(null);
			}

			ArrayList<Statement> myList = new ArrayList();
			for (Value graph : g) {
				// send the query to HBase
				//System.out.println("HBase Query: " + arg0 + " - " + arg1 + " - " + arg2 + " - " + graph);
				Value[] query = { arg0, arg1, arg2, graph };
				
				ArrayList<ArrayList<Value>> result = null;
				try {
					// reconstruct Statement objects
					result = hbase.opsManager.getResults(query);
					myList.addAll(reconstructTriples(result, query));
				}
				catch (Exception e) {
					// no result retrieved from HBase
				}
			}

			try {
				Iterator it = myList.iterator();
				CloseableIteration<Statement, SailException> ci = new CloseableIteratorIteration<Statement, SailException>(
						it);
				return ci;
			}
			catch (Exception e) {
				// if there are no results to retrieve
				return null;
			}

		} catch (Exception e) {
			Exception ex = new SailException("HBase connection error: " + e.getMessage());
			try {
				throw ex;
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		return null;
	}

	/**
	 * This function reconstructs RDF Statements from the list of Value items
	 * returned from HBase.
	 * 
	 * @param result
	 * @param triple
	 * @return
	 * @throws SailException
	 */
	protected ArrayList<Statement> reconstructTriples(ArrayList<ArrayList<Value>> result, Value[] triple)
			throws SailException {
		ArrayList<Statement> list = new ArrayList();

		for (ArrayList<Value> arrayList : result) {
			int index = 0;

			Resource s = null;
			URI p = null;
			Value o = null;
			Resource c = null;

			for (Value value : arrayList) {
				if (index == 0) {
					// s = (Resource)getSubject(value);
					s = (Resource) value;
				} else if (index == 1) {
					// p = (URI) getPredicate(value);
					p = (URI) value;
				} else if (index == 2) {

					// o = getObject(value);
					o = value;
				} else {
					// c = (Resource)getContext(value);
					c = (Resource) value;
					Statement statement = new ContextStatementImpl(s, p, o, c);
					list.add(statement);
				}
				index++;
			}
		}
		return list;
	}

	/**
	 * This functions parses a String to return the subject in a query.
	 * 
	 * @param s
	 * @return
	 */
	Value getSubject(String s) {
//		System.out.println("SUBJECT: " + s);
		if (s.startsWith("_")) {
			return new BNodeImpl(s.substring(2));
		}
		return new URIImpl(s);
	}

	/**
	 * This functions parses a String to return the predicate in a query.
	 * 
	 * @param s
	 * @return
	 */
	Value getPredicate(String s) {
//		System.out.println("PREDICATE: " + s);
		return new URIImpl(s);
	}

	/**
	 * This functions parses a String to return the object in a query.
	 * 
	 * @param s
	 * @return
	 */
	Value getObject(String s) {
//		System.out.println("OBJECT: " + s);
		if (s.startsWith("_")) {
			return new BNodeImpl(s.substring(2));
		} else if (s.startsWith("\"")) {
			String literal = "";
			String language = "";
			String datatype = "";

			for (int i = 1; i < s.length(); i++) {
				while (s.charAt(i) != '"') {

					// read literal value
					literal += s.charAt(i);
					if (s.charAt(i) == '\\') {
						i++;
						literal += s.charAt(i);
					}
					i++;
					if (i == s.length()) {
						// EOF exception
					}
				}
				// System.out.println(literal);

				// charAt(i) = '"', read next char
				i++;

				if (s.charAt(i) == '@') {
					// read language
					// System.out.println("reading language");
					i++;
					while (i < s.length()) {
						language += s.charAt(i);
						i++;
					}
					// System.out.println(language);
					return new LiteralImpl(literal, language);
				} else if (s.charAt(i) == '^') {
					// read datatype
					i++;

					// check for second '^'
					if (i == s.length()) {
						// EOF exception
					} else if (s.charAt(i) != '^') {
						// incorrect formatting exception
					}
					i++;

					// check for '<'
					if (i == s.length()) {
						// EOF exception
					} else if (s.charAt(i) != '<') {
						// incorrect formatting exception
					}
					i++;

					while (s.charAt(i) != '>') {
						datatype += s.charAt(i);
						i++;
						if (i == s.length()) {
							// EOF exception
						}
					}
					// System.out.println(datatype);
					return new LiteralImpl(literal, new URIImpl(datatype));
				} else {
					return new LiteralImpl(literal);
				}

			}
		}

		Value object;
		try {
			object = new URIImpl(s);
		} catch (Exception e) {
			object = new LiteralImpl(s);
		}
		return object;
	}

	/**
	 * This functions parses a String to return the graph in a query.
	 * 
	 * @param s
	 * @return
	 */
	Value getContext(String s) {
//		System.out.println("GRAPH: " + s);
		return new URIImpl(s);
	}

	@Override
	protected void removeNamespaceInternal(String arg0) throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void removeStatementsInternal(Resource arg0, URI arg1, Value arg2, Resource... arg3) throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void rollbackInternal() throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void setNamespaceInternal(String arg0, String arg1) throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected long sizeInternal(Resource... arg0) throws SailException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	protected void startTransactionInternal() throws SailException {
		// TODO Auto-generated method stub

	}

	/**
	 * This function retrieves all the triples from HBase that match with
	 * StatementPatterns in the SPARQL query, without executing the SPARQL query
	 * on them.
	 *
	 * @param arg0
	 * @return
	 * @throws SailException
	 */
	protected ArrayList<Statement> evaluateInternal(TupleExpr arg0, Dataset context) throws SailException {
		ArrayList<Statement> result = new ArrayList();

		try {
			// get statement patterns from query
			ArrayList<ArrayList<Var>> statements = HBaseQueryVisitor.convertToStatements(arg0, null, null);

			// retrieve default/named context list
			Set<URI> contexts = new HashSet();
			try {			
				// System.out.println("DATASET: " + context.toString());
				Set<URI> defGraphs = context.getDefaultGraphs();
				if (defGraphs != null && defGraphs.size() != 0) {
					for (URI gr : defGraphs) {
						contexts.add(gr);
					}
				}

				Set<URI> namedGraphs = context.getNamedGraphs();
				if (namedGraphs != null && namedGraphs.size() != 0) {
					for (URI gr : namedGraphs) {
						contexts.add(gr);
					}
				}
			}
			catch (Exception e) {
			//	e.printStackTrace();
				// no contexts found
				System.out.println("NO DEFAULT/NAMED CONTEXTS FOUND");
			}

			/* if (contexts != null && contexts.size() != 0) {
				for (URI gr : contexts) {
					System.out.println("CONTEXT FOUND: " + gr.stringValue());
				}
			} */

			Iterator it = statements.iterator();
			while (it.hasNext()) {
				
				ArrayList<Var> sp = (ArrayList<Var>) it.next();

				Resource subj = null;
				URI pred = null;
				Value obj = null;
				Iterator jt = sp.iterator();

				int index = 0;

				// convert Var objects to Sesame Value objects
				Set<URI> statementContexts = new HashSet<URI>();
				if (contexts != null && contexts.size() != 0) {
					statementContexts.addAll(contexts);
				}
				while (jt.hasNext()) {   // subject predict object
					
					Var var = (Var) jt.next();

					if (index == 0) {
						if (var.hasValue()) {
							subj = (Resource) getSubject(var.getValue().stringValue());
						} else if (var.isAnonymous()) {
							subj = (Resource) getSubject(var.getName());

						}
					} else if (index == 1) {
						if (var.hasValue()) {
							pred = (URI) getPredicate(var.getValue().stringValue());
						}

					} else if (index == 2) {
						if (var.hasValue()) {
							obj = getObject(var.getValue().toString());
						} else if (var.isAnonymous()) {
							obj = getObject(var.getName());
						}
					} else {
						if (var != null && var.hasValue()) {
							statementContexts.add((URI)getContext(var.getValue().toString()));
							// System.out.println("GRAPH: " + var.getValue().toString());
						}
					}
					index += 1;
				}

				// get the quads from HBase
				CloseableIteration ci = getStatementsInternal(subj, pred, obj, false, statementContexts);

				while (ci.hasNext()) {
					Statement statement = (Statement) ci.next();
					result.add(statement);
				}
			}
		} catch (Exception e) {
			throw new SailException(e);
		}

		return result;
	}

	
	@Override
	protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluateInternal(TupleExpr arg0,
			Dataset arg1, BindingSet arg2, boolean arg3) throws SailException {
		return null;
	}

	/**
	 * This function retrieves the relevant triples from HBase, loads them into
	 * an in-memory store, then evaluates the SPARQL query on them.
	 * 
	 * @param tupleExpr
	 * @param dataset
	 * @param bindings
	 * @param includeInferred
	 * @return
	 * @throws SailException
	 */
	public TupleQueryResult query(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred)
			throws SailException {
		// System.out.println("Evaluating query");
		// System.out.println("EVALUATE:" + tupleExpr.toString());

		try {
			ArrayList<Statement> statements = evaluateInternal(tupleExpr, dataset);
			// System.out.println("Statements retrieved: " + statements.size());

			Iterator it = statements.iterator();
			while (it.hasNext()) {
				Statement statement = (Statement) it.next();
//				System.out.println("WE GOT THIS SENTENCE: " + statement.toString());
				try {
					memStoreCon.addStatement(statement.getSubject(), statement.getPredicate(), statement.getObject(),
								statement.getContext());
//					System.out.println("CONTEXT FOR MEMORY STORE: " + statement.getContext().stringValue());
				} catch (Exception e) {
					memStoreCon.addStatement(statement.getSubject(), statement.getPredicate(), statement.getObject(),
							new URIImpl("http://hbase.sail.vu.nl"));
				}
			}
			memStoreCon.commit();

			CloseableIteration<? extends BindingSet, QueryEvaluationException> ci = memStoreCon.evaluate(tupleExpr,
					dataset, bindings, includeInferred);
			CloseableIteration<? extends BindingSet, QueryEvaluationException> cj = memStoreCon.evaluate(tupleExpr,
					dataset, bindings, includeInferred);

			List<String> bindingList = new ArrayList<String>();
			int index = 0;
			while (ci.hasNext()) {
				index++;
				BindingSet bs = ci.next();
				Set<String> localBindings = bs.getBindingNames();
				Iterator jt = localBindings.iterator();
				while (jt.hasNext()) {
					String binding = (String) jt.next();
					if (bindingList.contains(binding) == false) {
						bindingList.add(binding);
					}
				}
			}

			TupleQueryResult result = new TupleQueryResultImpl(bindingList, cj);

			return result;

		} catch (SailException e) {
			e.printStackTrace();
			throw e;
		} catch (QueryEvaluationException e) {
			throw new SailException(e);
		}
	}

	@Override
	protected CloseableIteration<? extends Statement, SailException> getStatementsInternal(Resource arg0, URI arg1,
			Value arg2, boolean arg3, Resource... arg4) throws SailException {
		// TODO Auto-generated method stub
		return null;
	}
}