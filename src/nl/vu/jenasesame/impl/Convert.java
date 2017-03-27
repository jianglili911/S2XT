package nl.vu.jenasesame.impl;


import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.openrdf.model.*;

/**
 * Created by jianglili on 2016/12/12.
 */
public class Convert {



    public static Node valueToNode(Value value){

        if ( value instanceof Literal )
            return literalToNode((Literal)value) ;
        if ( value instanceof URI )
            return uriToNode((URI)value) ;
        if ( value instanceof BNode )
            return bnodeToNode((BNode)value) ;
        throw new IllegalArgumentException("Not a concrete value") ;
    }


    public static Node bnodeToNode(BNode value)
    {
        return NodeFactory.createAnon(value.getID()) ;
    }

    public static Node uriToNode(URI value)
    {
        return NodeFactory.createURI(value.stringValue()) ;
    }
    public static Node literalToNode(Literal value)
    {
        if ( value.getLanguage() != null )
            return NodeFactory.createLiteral(value.getLabel(), value.getLanguage(), false) ;
        if ( value.getDatatype() != null )
            return NodeFactory.createLiteral(value.getLabel(),null, NodeFactory.getType(value.getDatatype().stringValue())) ;
        // Plain literal
        return NodeFactory.createLiteral(value.getLabel()) ;
    }

    public static Triple statementToTriple(Statement stmt)
    {
        Node s = Convert.valueToNode(stmt.getSubject()) ;
        Node p = Convert.uriToNode(stmt.getPredicate()) ;
        Node o = Convert.valueToNode(stmt.getObject()) ;
        return new Triple(s,p,o) ;
    }

    // ----
    // Problems with the ValueFactory

    public static Value nodeToValue(ValueFactory factory, Node node)
    {
        if ( node.isLiteral() )
            return nodeLiteralToValue(factory, node) ;
        if ( node.isURI() )
            return nodeURIToValue(factory, node) ;
        if ( node.isBlank() )
            return nodeBlankToValue(factory, node) ;
        throw new IllegalArgumentException("Not a concrete node") ;
    }

    public static Resource nodeToValueResource(ValueFactory factory, Node node)
    {
        if ( node.isURI() )
            return nodeURIToValue(factory, node) ;
        if ( node.isBlank() )
            return nodeBlankToValue(factory, node) ;
        throw new IllegalArgumentException("Not a URI nor a blank node") ;
    }

    public static BNode nodeBlankToValue(ValueFactory factory, Node node)
    {
        return factory.createBNode(node.getBlankNodeLabel()) ;
    }

    public static URI nodeURIToValue(ValueFactory factory, Node node)
    {
        return factory.createURI(node.getURI()) ;
    }

    public static Value nodeLiteralToValue(ValueFactory factory, Node node)
    {
        if ( node.getLiteralDatatype() != null )
        {
            URI x = factory.createURI(node.getLiteralDatatypeURI()) ;
            return factory.createLiteral(node.getLiteralLexicalForm(), x) ;
        }
        if ( ! node.getLiteralLanguage().equals("") )
        {
            return factory.createLiteral(node.getLiteralLexicalForm(), node.getLiteralLanguage()) ;
        }

        return factory.createLiteral(node.getLiteralLexicalForm()) ;
    }

}
