package nl.vu.jena.sparql.engine.optimizer;

import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.op.OpLabel;
import org.apache.jena.sparql.algebra.optimize.*;
import org.apache.jena.sparql.algebra.optimize.Optimize.RewriterFactory;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.util.Context;

public class HBaseOptimize implements Rewrite {
	
	public static RewriterFactory hbaseOptimizationFactory = new RewriterFactory()
    {
        @Override
        public Rewrite create(Context context)
        {
            return new HBaseOptimize(context) ;
        }
    } ;
    
    private final Context context ;
    
    public HBaseOptimize(ExecutionContext execCxt)
    {
        this(execCxt.getContext()) ;
    }

	public HBaseOptimize(Context context) {
		this.context = context;
	}

	@Override
	public Op rewrite(Op op) {
		// Record optimizer
        if ( context.get(ARQConstants.sysOptimizer) == null )
            context.set(ARQConstants.sysOptimizer, this) ;

        // ** TransformScopeRename::
        // This is a requirement for the linearization execution that the default
        // ARQ query engine uses where possible.  
        // This transformation must be done (e.g. by QueryEngineBase) if no other optimziation is done. 
        op = TransformScopeRename.transform(op) ;
        
        // Remove "group of one" join
        // Done in AlgebraGenerator
        // e..g CONSTRUCT {} WHERE { SELECT ... } 
        //op = TransformTopLevelSelect.simplify(op) ;
        
        // Prepare expressions.
        OpWalker.walk(op, new OpVisitorExprPrepare(context)) ;
        
        // Need to allow subsystems to play with this list.
        
        if ( context.isTrueOrUndef(ARQ.propertyFunctions) )
            op = Optimize.apply("Property Functions", new TransformPropertyFunction(context), op) ;

        if ( context.isTrueOrUndef(ARQ.optFilterConjunction) )
            op = Optimize.apply("filter conjunctions to ExprLists", new TransformFilterConjunction(), op) ;

        if ( context.isTrueOrUndef(ARQ.optFilterExpandOneOf) )
            op = Optimize.apply("Break up IN and NOT IN", new TransformExpandOneOf(), op) ;

        // TODO Improve filter placement to go through assigns that have no effect.
        // Either, do filter placement and other sequence generating transformations.
        // or improve to place in a sequence (latter is better?)
        
        /*TODO eliminated this to work with jena-arq.2.9.2
         * if ( context.isTrueOrUndef(ARQ.optFilterEquality) )
        {
            //boolean termStrings = context.isDefined(ARQ.optTermStrings) ;
            op = Optimize.apply("Filter Equality", new TransformFilterEquality(), op) ;
        }*/
        
        if ( context.isTrueOrUndef(ARQ.optFilterDisjunction) )
            op = Optimize.apply("Filter Disjunction", new TransformFilterDisjunction(), op) ;
        
        if ( context.isTrueOrUndef(ARQ.optFilterPlacement) )
            // This can be done too early (breaks up BGPs).
            op = Optimize.apply("Filter Placement", new TransformFilterPlacement(), op) ;
        else{
        	//assume we have a setup with another filter placement strategy 
        	op = Optimize.apply("Filter Placement", (Transform)context.get(ARQ.optFilterPlacement), op);
        }
        
        if ( context.isTrueOrUndef(ARQ.optTopNSorting) )
        	op = Optimize.apply("TopN Sorting", new TransformTopN(), op) ;

        if ( context.isTrueOrUndef(ARQ.optDistinctToReduced) )
            op = Optimize.apply("Distinct replaced with reduced", new TransformDistinctToReduced(), op) ;
        
        // Convert paths to triple patterns. 
        // Also done in the AlgebraGenerator so this transform step catches programattically built op expressions 
        op = Optimize.apply("Path flattening", new TransformPathFlattern(), op) ;
        
        // Find joins/leftJoin that can be done by index joins (generally preferred as fixed memory overhead).
        op = Optimize.apply("Join strategy", new TransformJoinStrategy(), op) ;
        
        op = Optimize.apply("Merge BGPs", new TransformMergeBGPs(), op) ;
        
        // Mark
        if ( false )
            op = OpLabel.create("Transformed", op) ;
        return op ;
	}

}
