package de.tf.uni.freiburg.sparkrdf.parser.query.expression.op;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.util.NodeFactoryExtra;

import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;

/**
 * @author Thorsten Berberich
 */
public class Add extends Expr2 implements IValueType {

    /**
     * Generated UID
     */
    private static final long serialVersionUID = -8493226342978420707L;

    public Add(IExpression left, IExpression right) {
        super(left, right);
    }

    @Override
    public Boolean evaluate(SolutionMapping solution) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getValue(SolutionMapping solution) {
        String left = "";
        String right = "";
        if (expr1 instanceof ExprVar) {
            left = solution.getValueToField(((ExprVar) expr1).getVar());
        } else {
            left = ((IValueType) expr1).getValue(solution);
        }

        if (expr2 instanceof ExprVar) {
            right = solution.getValueToField(((ExprVar) expr2).getVar());
        } else {
            right = ((IValueType) expr2).getValue(solution);
        }

        Node nodeLeft = NodeFactoryExtra.parseNode(left);
        Node nodeRight = NodeFactoryExtra.parseNode(right);
        Integer leftInt = (Integer) nodeLeft.getLiteral().getValue();
        Integer rightInt = (Integer) nodeRight.getLiteral().getValue();

        int res = leftInt + rightInt;

        return String.format(
                "\"%s\"^^<http://www.w3.org/2001/XMLSchema#integer>", res);
    }

}
