package nl.vu.jena.sparql.engine.optimizer;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Variable;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.*;
import org.apache.jena.sparql.util.VarUtils;
import nl.vu.jena.graph.FilteredTriple;

import java.util.*;

public class HBaseTransformFilterPlacement extends TransformCopy {
	static boolean doFilterPlacement = true ;
	
	private static List<Class> supportedFunctionForSimpleFilters = Arrays.asList(new Class[]{ E_Equals.class,
			E_LessThanOrEqual.class,
			E_LessThan.class,
			E_GreaterThan.class,
			E_GreaterThanOrEqual.class});
    
    public static Op transform(ExprList exprs, BasicPattern bgp)
    {
        if ( ! doFilterPlacement )
            return OpFilter.filter(exprs, new OpBGP(bgp)) ;
        
        Op op = transformFilterBGP(exprs, new HashSet<Var>(), bgp) ;
        // Remaining filters? e.g. ones mentioning var s not used anywhere. 
        op = buildFilter(exprs, op) ;
        return op ;
    }
    
    public static Op transform(ExprList exprs, Node graphNode, BasicPattern bgp)
    {
        if ( ! doFilterPlacement )
            return OpFilter.filter(exprs, new OpQuadPattern(graphNode, bgp)) ;
        Op op =  transformFilterQuadPattern(exprs, new HashSet<Var>(), graphNode, bgp);
        op = buildFilter(exprs, op) ;
        return op ;
    }
    

    public HBaseTransformFilterPlacement()
    { }
    
    @Override
    public Op transform(OpFilter opFilter, Op x)
    {
        if ( ! doFilterPlacement )
            return super.transform(opFilter, x) ;
        
        // Destructive use of exprs - copy it.
        ExprList exprs = new ExprList(opFilter.getExprs().getList()) ;
        Set<Var> varsScope = new HashSet<Var>() ;
        
        Op op = transform(exprs, varsScope, x) ;
        if ( op == x )
            // Didn't do anything.
            return super.transform(opFilter, x) ;
        
        // Remaining exprs
        op = buildFilter(exprs, op) ;
        return op ;
    }
    
    private static Op transform(ExprList exprs, Set<Var> varsScope, Op x)
    {
        // OpAssign/OpExtend could be done if the assignment and exprs are independent.
        // TODO Dispatch by visitor??
        if ( x instanceof OpBGP )
            return transformFilterBGP(exprs, varsScope, (OpBGP)x) ;

        if ( x instanceof OpSequence )
            return transformFilterSequence(exprs, varsScope, (OpSequence)x) ;
        
        if ( x instanceof OpQuadPattern )
            return transformFilterQuadPattern(exprs, varsScope, (OpQuadPattern)x) ;
        
        if ( x instanceof OpSequence )
            return transformFilterSequence(exprs, varsScope, (OpSequence)x) ;
        
        if ( x instanceof OpConditional )
            return transformFilterConditional(exprs, varsScope, (OpConditional)x) ;
        
        // Not special - advance the variable scope tracking. 
       // OpVars.patternVars(x, varsScope) ;
        OpVars.visibleVars(x, varsScope) ;
        return x ;
    }
    
    // == The transformFilter* modify the exprs and patternVarsScope arguments 
    
    private static Op transformFilterBGP(ExprList exprs, Set<Var> patternVarsScope, OpBGP x)
    {
        return  transformFilterBGP(exprs, patternVarsScope, x.getPattern()) ;
    }

    private static List<Triple> transformSimpleFilterBGP(Expr expr, List<Triple> inputTriplePatterns/*list of triple patterns*/){
    	//assuming we have simple filter expressions
    	
    	List<Triple> modifiedTriplePatterns = new ArrayList<Triple>(inputTriplePatterns.size());
    	
    	//Expr expr = exprs.get(exprs.size()-1);
    	Set<Var> exprVars = expr.getVarsMentioned();
    	
    	boolean modified = false;
    	for (Triple triple : inputTriplePatterns) {		
    		if (VarUtils.getVars(triple).containsAll(exprVars) && !(triple instanceof FilteredTriple)){
    			if (expr instanceof E_Equals){
    				//bind the constant in the triple
    				Triple newTriple = bindConstantInTriple((E_Equals)expr, triple);
    				modifiedTriplePatterns.add(newTriple);
    			}
    			else {
    				FilteredTriple fTriple = new FilteredTriple(triple, expr);
    				modifiedTriplePatterns.add(fTriple);
    			}
    			modified = true;
    		}
    		else{
    			modifiedTriplePatterns.add(triple);
    		}
		}
    	
    	if (modified){
    		return modifiedTriplePatterns;
    	} else {
    		return inputTriplePatterns;
    	}
    }
    
    private static Triple bindConstantInTriple(E_Equals expr, Triple triple) {
    	NodeValue val;
    	String varName;
		if (expr.getArg1().isConstant()){
			val = expr.getArg1().getConstant();
			varName = expr.getArg2().getExprVar().getVarName();
		}
		else if (expr.getArg2().isConstant()){
			val = expr.getArg2().getConstant();
			varName = expr.getArg1().getExprVar().getVarName();
		}
		else{
			throw new RuntimeException("bindConstantInTriple: this shouldn't happen");
		}
		
		Triple newTriple;
		if (triple.getSubject() instanceof Node_Variable && triple.getSubject().getName().equals(varName)){
			newTriple = new Triple(val.asNode(), triple.getPredicate(), triple.getObject());
		}
		else if (triple.getPredicate() instanceof Node_Variable && triple.getPredicate().getName().equals(varName)){
			newTriple = new Triple(triple.getSubject(), val.asNode(), triple.getObject());
		}
		else if (triple.getObject() instanceof Node_Variable && triple.getObject().getName().equals(varName)){
			newTriple = new Triple(triple.getSubject(), triple.getPredicate(), val.asNode());
		}
		else{
			throw new RuntimeException("bindConstantInTriple: No triple position bound by expression value");
		}
		
		return newTriple;
	}

	private static Op transformFilterBGP(ExprList exprs, Set<Var> patternVarsScope, BasicPattern pattern)
    {
		List<Triple> triplePatterns = pattern.getList();
		for (Iterator<Expr> exprIt = exprs.iterator(); exprIt.hasNext();) {
			Expr expr = (Expr) exprIt.next();
			if (isSimpleFilter(expr)){
				List<Triple> modifiedTriplePatterns = transformSimpleFilterBGP(expr, triplePatterns);
				if (modifiedTriplePatterns != triplePatterns){
					exprIt.remove();
				}
				triplePatterns = modifiedTriplePatterns;
	    	}
		}
		
		if (exprs.isEmpty()){
			new OpBGP(BasicPattern.wrap(triplePatterns));
		}
    
        // Any filters that depend on no variables. 
        Op op = insertAnyFilter(exprs, patternVarsScope, null) ;
        
        for ( Triple triple : triplePatterns )
        {
            OpBGP opBGP = getBGP(op) ;
            if ( opBGP == null )
            {
                // Last thing was not a BGP (so it likely to be a filter)
                // Need to pass the results from that into the next triple.
                // Which is a join and sequence is a special case of join
                // which always evaluates by passing results of the early
                // part into the next element of the sequence.
                
                opBGP = new OpBGP() ;    
                op = OpSequence.create(op, opBGP) ;
            }
            
            opBGP.getPattern().add(triple) ;
            // Update variables in scope.
            VarUtils.addVarsFromTriple(patternVarsScope, triple) ;
            
            // Attempt to place any filters
            op = insertAnyFilter(exprs, patternVarsScope, op) ;
        } 
        // Leave any remaining filter expressions - don't wrap up any as something else may take them.
        return op ;
    }
    
    private static boolean isSimpleFilter(Expr expr) {
		/*if (exprs.size() != 1){
			return false;
		}*/
		
		//Expr expr = exprs.get(exprs.size()-1);
		if (!(expr instanceof ExprFunction2))
			return false;
		
		ExprFunction2 exFunc2 = (ExprFunction2)expr;
		if (((exFunc2.getArg1().isConstant() && exFunc2.getArg1().getConstant().isNumber()) || 
				(exFunc2.getArg2().isConstant() && exFunc2.getArg2().getConstant().isNumber())) 
				&& supportedFunctionForSimpleFilters.contains(exFunc2.getClass())){
			return true;
		}
		
		return false;
	}

	/** Find the current OpBGP, or return null. */ 
    private static OpBGP getBGP(Op op)
    {
        if ( op instanceof OpBGP )
            return (OpBGP)op ;
        
        if ( op instanceof OpSequence )
        {
            // Is last in OpSequence an BGP?
            OpSequence opSeq = (OpSequence)op ;
            List<Op> x = opSeq.getElements() ;
            if ( x.size() > 0 )
            {                
                Op opTop = x.get(x.size()-1) ;
                if ( opTop instanceof OpBGP )
                    return (OpBGP)opTop ;
                // Drop through
            }
        }
        // Can't find.
        return null ;
    }
    
    private static Op transformFilterQuadPattern(ExprList exprs, Set<Var> patternVarsScope, OpQuadPattern pattern)
    {
        return transformFilterQuadPattern(exprs, patternVarsScope, pattern.getGraphNode(), pattern.getBasicPattern()) ;
    }
    
    private static Op transformFilterQuadPattern(ExprList exprs, Set<Var> patternVarsScope, Node graphNode, BasicPattern pattern) 
    {
        // Any filters that depend on no variables. 
        Op op = insertAnyFilter(exprs, patternVarsScope, null) ;
        if ( Var.isVar(graphNode) )
        {
            // Add in the graph node of the quad block.
            // It's picked up after the first triple is processed.
            VarUtils.addVar(patternVarsScope, Var.alloc(graphNode)) ;
        }
        
        for ( Triple triple : pattern )
        {
            OpQuadPattern opQuad = getQuads(op) ;
            if ( opQuad == null )
            {
                opQuad = new OpQuadPattern(graphNode, new BasicPattern()) ;    
                op = OpSequence.create(op, opQuad) ;
            }
            
            opQuad.getBasicPattern().add(triple) ;
            // Update variables in scope.
            VarUtils.addVarsFromTriple(patternVarsScope, triple) ;

            // Attempt to place any filters
            op = insertAnyFilter(exprs, patternVarsScope, op) ;
        }
        
        
        return op ;
    }
    
    /** Find the current OpQuadPattern, or return null. */ 
    private static OpQuadPattern getQuads(Op op)
    {
        if ( op instanceof OpQuadPattern )
            return (OpQuadPattern)op ;
        
        if ( op instanceof OpSequence )
        {
            // Is last in OpSequence an BGP?
            OpSequence opSeq = (OpSequence)op ;
            List<Op> x = opSeq.getElements() ;
            if ( x.size() > 0 )
            {                
                Op opTop = x.get(x.size()-1) ;
                if ( opTop instanceof OpQuadPattern )
                    return (OpQuadPattern)opTop ;
                // Drop through
            }
        }
        // Can't find.
        return null ;
    }

    private static Op transformFilterSequence(ExprList exprs, Set<Var> varScope, OpSequence opSequence)
    {
        List<Op> ops = opSequence.getElements() ;
        
        // Any filters that depend on no variables. 
        Op op = insertAnyFilter(exprs, varScope, null) ;
        
        for ( Iterator<Op> iter = ops.iterator() ; iter.hasNext() ; )
        {
            Op seqElt = iter.next() ;
            // Process the sequence element.  This may insert filters (sequence or BGP)
            seqElt = transform(exprs, varScope, seqElt) ;
            // Merge into sequence.
            op = OpSequence.create(op, seqElt) ;
            // Place any filters now ready.
            op = insertAnyFilter(exprs, varScope, op) ;
        }
        return op ;
    }
    
    // Modularize.
    private static Op transformFilterConditional(ExprList exprs, Set<Var> varScope, OpConditional opConditional)
    {
        // Any filters that depend on no variables. 
        Op op = insertAnyFilter(exprs, varScope, null) ;
        Op left = opConditional.getLeft();
        left = transform(exprs, varScope, left);
        Op right = opConditional.getRight();
        op = new OpConditional(left, right);
        op = insertAnyFilter(exprs, varScope, op);
        return op;
     }
    
    // ---- Utilities
    
    /** For any expression now in scope, wrap the op with a filter */
    private static Op insertAnyFilter(ExprList exprs, Set<Var> patternVarsScope, Op op)
    {
        for ( Iterator<Expr> iter = exprs.iterator() ; iter.hasNext() ; )
        {
            Expr expr = iter.next() ;
            // Cache
            Set<Var> exprVars = expr.getVarsMentioned() ;
            if ( patternVarsScope.containsAll(exprVars) && !(expr instanceof E_NotEquals) )
            {
                if ( op == null )
                    op = OpTable.unit() ;
                op = OpFilter.filter(expr, op) ;
                iter.remove() ;
            }
        }
        return op ;
    }
    
    /** Place expressions around an Op */ 
    private static Op buildFilter(ExprList exprs, Op op)
    {
        if ( exprs.isEmpty() )
            return op ;
    
        for ( Iterator<Expr> iter = exprs.iterator() ; iter.hasNext() ; )
        {
            Expr expr = iter.next() ;
            if ( op == null )
                op = OpTable.unit() ;
            op = OpFilter.filter(expr, op) ;
            iter.remove();
        }
        return op ;
    }
}
