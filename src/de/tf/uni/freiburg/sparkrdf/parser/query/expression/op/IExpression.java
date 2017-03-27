package de.tf.uni.freiburg.sparkrdf.parser.query.expression.op;

import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;

import java.io.Serializable;

/**
 * Created by jianglili on 2016/5/8.
 */
public interface IExpression extends Serializable {

    public abstract Boolean evaluate(SolutionMapping solution);
}
