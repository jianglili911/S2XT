package nl.vu.datalayer.hbase.sail;

import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A SPARQL query visitor, it is used to parse SPARQL queries and return
 * statement patterns.
 * 
 * @author Anca Dumitrache
 * @see <a href="https://github.com/openphacts/OpsPlatform/blob/master/ops-platform/larkc-plugins/plugin.imsSqarqlExpand/src/main/java/eu/ops/plugin/imssparqlexpand/querywriter/QueryWriterModelVisitor.java">QueryWriterModelVisitor in OPS</a>
 */
public class HBaseQueryVisitor implements QueryModelVisitor<QueryExpansionException> {

	// Builder to write the query to bit by bit
	StringBuilder queryString = new StringBuilder();

	// Statement patterns to be retrieved from HBase
	ArrayList<ArrayList<Var>> statements;

	// Used to add the FROM clause
	private Dataset originalDataSet;

	// The current context (GRAPH clause) the writer is in. (or null if not in a
	// context.
	Var context = null;

	// List of attributes to be used or null to include all.
	// WARNING: this functionality is in an early stage of development
	// so can only handle queries where each attribute comes from exactly one
	// statement.
	private List<String> requiredAttributes;

	// List of attributes to be removed. See requiredAttributes warning!
	private Set<String> eliminatedAttributes;

	// Flag that the writing of an Optional has been delayed until a Graph
	// clause is added
	boolean swapGraphAndOptional = false;

	// List of the Contexts (including null) for all the Statements not yet met.
	ArrayList<Var> contexts;

	// List of the number of options clauses pushed under the graph clause.
	int optionInGraph = 0;

	/**
	 * Sets up the visitor for writing the query.
	 * 
	 * @param dataSet
	 *            dataSets listed in the original Queries FROM clause.
	 * @param requiredAttributes
	 *            List of attributes to be used or null to include all.
	 * @param contexts
	 *            List of Contexts retrieved using the ContextListerVisitor.
	 */
	HBaseQueryVisitor(Dataset dataSet, List<String> requiredAttributes, ArrayList<Var> contexts) {
		originalDataSet = dataSet;
		eliminatedAttributes = new HashSet<String>();
		if (requiredAttributes == null || requiredAttributes.isEmpty()) {
			this.requiredAttributes = null;
		} else {
			this.requiredAttributes = requiredAttributes;
		}
		this.contexts = contexts;

		statements = new ArrayList();
	}

	public static ArrayList<Var> getContexts(TupleExpr tupleExpr) throws QueryExpansionException {
		return ContextListerVisitor.getContexts(tupleExpr);
	}

	/**
	 * Sets up the visitor for writing the query.
	 * 
	 * @param dataSet
	 *            dataSets listed in the original Queries FROM clause.
	 * @param contexts
	 *            List of Contexts retrieved using the ContextListerVisitor.
	 */
	HBaseQueryVisitor(Dataset dataSet, ArrayList<Var> contexts) {
		this(dataSet, null, contexts);
	}

	@Override
	public void meet(QueryRoot arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(And arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(BNodeGenerator arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Bound arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Compare arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(CompareAll arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(CompareAny arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}


	@Override
	public void meet(Count arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Datatype arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Difference arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Distinct arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(EmptySet arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Exists arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Extension arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(ExtensionElem arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(FunctionCall arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Group arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(GroupElem arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(In arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Intersection arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(IsBNode arg0) throws QueryExpansionException {
		queryString.append(" ISBLANK(");
		arg0.getArg().visit(this);
		queryString.append(")");
	}

	@Override
	public void meet(IsLiteral arg0) throws QueryExpansionException {
		queryString.append(" ISLITERAL(");
		arg0.getArg().visit(this);
		queryString.append(")");
	}

	@Override
	public void meet(IsResource arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(IsURI arg0) throws QueryExpansionException {
		queryString.append(" ISIRI(");
		arg0.getArg().visit(this);
		queryString.append(")");
	}

	@Override
	public void meet(Join arg0) throws QueryExpansionException {
		// System.out.println("Found Join");
		arg0.getLeftArg().visit(this);
		arg0.getRightArg().visit(this);
	}

	@Override
	public void meet(Label arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Lang arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(LangMatches arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Like arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(LocalName arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(MathExpr arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Max arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Min arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(MultiProjection arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Namespace arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Not arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	// @Override
	/**
	 * 
	 * @param lj
	 * @throws QueryExpansionException
	 */
	@Override
	public void meet(LeftJoin lj) throws QueryExpansionException {
		// System.out.println("Found left join");
		// The leftArg is the stuff outside of the optional.
		// May be a SingletonSet in which case nothing is written
		lj.getLeftArg().visit(this);
		lj.getRightArg().visit(this);
	}

	/**
	 * Preforms a look ahead to see how many statements will be in the next
	 * Graph clause.
	 * <p>
	 * In practice it uses the list of contexts passed in the constructor to do
	 * the look ahead. This was done as the next statements may be in a
	 * completely different branch of the tree.
	 * 
	 * @return Number of statements in the Graph Clause
	 */
	private int statementsInNextGraph() {
		// Starts at the 0 element of the contexts list as meet(Statement)
		// removes context that have been met.
		// If you are not in a grahp just return 0
		if (contexts.get(0) == null) {
			return 0;
		}
		// Use the list of count how many are the same.
		int i;
		for (i = 0; i < contexts.size(); i++) {
			if (!(contexts.get(0).equals(contexts.get(i)))) {
				// Found one different so return the count
				return i;
			}
		}
		// End of the list so return the count.
		return i;
	}

	/**
	 * Counts the number of statements in this query or sub query.
	 * 
	 * @param tupleExpr
	 *            query or sub query
	 * @return Number of statements met.
	 * @throws QueryExpansionException
	 *             Unlikely but just in case.
	 */
	private int statementsInExpression(TupleExpr tupleExpr) throws QueryExpansionException {
		// Could be done with a lister that counts instead of lists but this
		// reuses code.
		ArrayList<Var> localContexts = ContextListerVisitor.getContexts(tupleExpr);
		return localContexts.size();
	}

	@Override
	public void meet(Or arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Order arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(OrderElem arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Projection prjctn) throws QueryExpansionException {
		meet(prjctn, "");
	}

	/**
	 * Used by sub classes to add expaned list if required.
	 * 
	 * Currently not used but left in if required again.
	 */
	void addExpanded(Projection prjctn) throws QueryExpansionException {
	}

	public void meet(Projection prjctn, String modifier) throws QueryExpansionException {
		queryString.append("SELECT ");
		queryString.append(modifier);
		addExpanded(prjctn);
		if (this.requiredAttributes != null) {
			for (String requiredAttribute : requiredAttributes) {
				queryString.append(" ?");
				queryString.append(requiredAttribute);
			}
		}
		// Call the ProjectionElementList even if there are requiredAttributes
		// as this sets eliminatedAttributes
		prjctn.getProjectionElemList().visit(this);
		newLine();
		queryString.append("WHERE {");
		prjctn.getArg().visit(this);
		// closeProjectionUnlessOrderHas(prjctn.getArg());
	}

	@Override
	public void meet(ProjectionElemList arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub
		// System.out.println("FOUND ProjectionElemList");

	}

	@Override
	public void meet(ProjectionElem arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub
		// System.out.println("FOUND ProjectionElem");
	}

	@Override
	public void meet(Reduced arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Regex arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Slice arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(SameTerm arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Filter arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(SingletonSet arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	// @Override
	@Override
	public void meet(StatementPattern sp) throws QueryExpansionException {
//		System.out.println("Found StatementPattern");
		ArrayList<Var> statement = new ArrayList();

		statement.add(sp.getSubjectVar());
		statement.add(sp.getPredicateVar());
		statement.add(sp.getObjectVar());
		statement.add(sp.getContextVar());

		statements.add(statement);
	}

	@Override
	public void meet(Str arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Union arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(ValueConstant arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}



	@Override
	public void meet(Var arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meetOther(QueryModelNode arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	void newLine() {
		queryString.append("\n");
	}

	/**
	 * Returns the query as a string.
	 * <p>
	 * Works if and only if the model was visited exactly once.
	 * 
	 * @return query as a String
	 * @throws QueryExpansionException
	 *             Declared as thrown to allow calling methods to catch it
	 *             specifically.
	 */
	private String getQuery() {
		return queryString.toString();
	}

	/**
	 * Returns a list of statement patterns from the processed SPARQL query.
	 * <p>
	 * The patterns are expressed as a list of Value objects, where a query
	 * variable is expressed as a null value.
	 * <p>
	 * Works if and only if the model was visited exactly once.
	 * 
	 * @return list of statement patterns
	 */
	private ArrayList<ArrayList<Var>> getStatements() {
		return statements;
	}

	/**
	 * Converts a SPARQL query in TupleExpr format to a list of statement patterns.
	 * <p>
	 * For now, only the TupleExpr parameter is relevant, all the information concerning
	 * the query can be extracted from there.
	 * 
	 * @param tupleExpr
	 * @param dataSet
	 * @param requiredAttributes
	 * @return
	 * @throws QueryExpansionException
	 */
	public static ArrayList<ArrayList<Var>> convertToStatements(TupleExpr tupleExpr, Dataset dataSet,
			List<String> requiredAttributes) throws QueryExpansionException {
//		System.out.println("Evaluating TupleExpr:" + tupleExpr.toString());

		ArrayList<Var> contexts = ContextListerVisitor.getContexts(tupleExpr);

		HBaseQueryVisitor writer = new HBaseQueryVisitor(dataSet, requiredAttributes, contexts);
		tupleExpr.visit(writer);
		return writer.getStatements();
	}

	@Override
	public void meet(Add arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(ArbitraryLengthPath arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Avg arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(BindingSetAssignment arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Clear arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Coalesce arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Copy arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Create arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(DeleteData arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(GroupConcat arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(If arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(InsertData arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(IRIFunction arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(IsNumeric arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Load arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Modify arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Move arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Sample arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Service arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(Sum arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}

	@Override
	public void meet(ZeroLengthPath arg0) throws QueryExpansionException {
		// TODO Auto-generated method stub

	}
	//new add
	@Override
	public void meet(DescribeOperator node) throws QueryExpansionException {
       // TODO Auto-generated method stub
	}
	@Override
	public void meet(ListMemberOperator node) throws QueryExpansionException {
		// TODO Auto-generated method stub
	}
}