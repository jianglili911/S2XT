package nl.vu.jena.sparql.engine.iterator;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.binding.BindingMap;
import org.apache.jena.sparql.engine.iterator.QueryIter;
import org.apache.jena.sparql.engine.iterator.QueryIterRepeatApply;
import org.apache.jena.util.iterator.ClosableIterator;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import nl.vu.jena.graph.TripleBinder;

public class QueryIterTriplePattern extends QueryIterRepeatApply {

private final Triple pattern ;
    
    public QueryIterTriplePattern( QueryIterator input,
                                   Triple pattern , 
                                   ExecutionContext cxt)
    {
        super(input, cxt) ;
        this.pattern = pattern ;
    }

    @Override
    protected QueryIterator nextStage(Binding binding)
    {
        return new TripleMapper(binding, pattern, getExecContext()) ;
    }
    
    static int countMapper = 0 ; 
    static class TripleMapper extends QueryIter
    {
        private Node s ;
        private Node p ;
        private Node o ;
        private Binding binding ;
        private ClosableIterator<Triple> graphIter ;
        private Binding slot = null ;
        private boolean finished = false ;
        private volatile boolean cancelled = false ;

        TripleMapper(Binding binding, Triple pattern, ExecutionContext cxt)
        {
            super(cxt) ;
            
            this.binding = binding ;
            Graph graph = cxt.getActiveGraph() ;
            
            Triple bindedPattern = TripleBinder.bindTriple(pattern, binding);
            this.s = bindedPattern.getSubject();
            this.p = bindedPattern.getPredicate();
            this.o = bindedPattern.getObject();
            
            ExtendedIterator<Triple> iter = graph.find(bindedPattern) ;
            
            this.graphIter = iter ;
        }

        private Binding mapper(Triple r)
        {
            BindingMap results = BindingFactory.create(binding) ;

            if ( ! insert(s, r.getSubject(), results) )
                return null ; 
            if ( ! insert(p, r.getPredicate(), results) )
                return null ;
            if ( ! insert(o, r.getObject(), results) )
                return null ;
            return results ;
        }

        private static boolean insert(Node inputNode, Node outputNode, BindingMap results)
        {
            if ( ! Var.isVar(inputNode) )
                return true ;
            
            Var v = Var.alloc(inputNode) ;
            Node x = results.get(v) ;
            if ( x != null )
                return outputNode.equals(x) ;
            
            results.add(v, outputNode) ;
            return true ;
        }
        
        @Override
        protected boolean hasNextBinding()
        {
            if ( finished ) return false ;
            if ( slot != null ) return true ;
            if ( cancelled )
            {
                graphIter.close() ;
                finished = true ;
                return false ;
            }

            while(graphIter.hasNext() && slot == null )
            {
                Triple t = graphIter.next() ;
                slot = mapper(t) ;
            }
            if ( slot == null )
                finished = true ;
            return slot != null ;
        }

        @Override
        protected Binding moveToNextBinding()
        {
            if ( ! hasNextBinding() ) 
                throw new ARQInternalErrorException() ;
            Binding r = slot ;
            slot = null ;
            return r ;
        }

        @Override
        protected void closeIterator()
        {
            if ( graphIter != null )
                NiceIterator.close(graphIter) ;
            graphIter = null ;
        }
        
        @Override
        protected void requestCancel()
        {
            // The QueryIteratorBase machinary will do the real work.
            cancelled = true ;
        }
    }

}
