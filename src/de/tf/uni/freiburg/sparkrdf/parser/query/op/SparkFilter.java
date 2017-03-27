package de.tf.uni.freiburg.sparkrdf.parser.query.op;

import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.expr.Expr;
import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.IntermediateResultsModel;
import de.tf.uni.freiburg.sparkrdf.parser.query.expression.ExprCompiler;
import de.tf.uni.freiburg.sparkrdf.parser.query.expression.op.IExpression;
import de.tf.uni.freiburg.sparkrdf.sparql.SparkFacade;
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;
import org.apache.spark.rdd.RDD;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by jianglili on 2016/5/8.
 */
public class SparkFilter implements SparkOp {

    private final OpFilter op;
    private final Set<IExpression> expressions;
    private final String TAG = "Filter";
    public SparkFilter(OpFilter op, PrefixMapping prefixes) {
        this.op = op;
        expressions = new HashSet<>();

        Iterator<Expr> iterator = op.getExprs().iterator();
        while (iterator.hasNext()) {
            Expr current = iterator.next();
            ExprCompiler translator = new ExprCompiler(prefixes);
            expressions.add(translator.translate(current));
        }
    }
    @Override
    public void execute() {
        if (op.getSubOp() != null) {
            RDD<SolutionMapping> result = IntermediateResultsModel
                    .getInstance().getResultRDD(op.getSubOp().hashCode());
            RDD<SolutionMapping> filteredRes = SparkFacade.filter(result,
                    this.expressions);

            IntermediateResultsModel.getInstance().putResult(
                    op.hashCode(),
                    filteredRes,
                    IntermediateResultsModel.getInstance().getResultVariables(
                            op.getSubOp().hashCode()));

            IntermediateResultsModel.getInstance().removeResult(
                    op.getSubOp().hashCode());
        }
    }

    @Override
    public String getTag() {
        return TAG;
    }
}
