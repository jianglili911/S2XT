package org.buaa.nlsde.jianglili.query.hbaserdf.OpToQueryTrans;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementUnion;
import org.buaa.nlsde.jianglili.reasoningquery.assertionReplace.*;

import java.util.Stack;

/**
 * Created by jianglili on 2017/2/4.
 */
public class AlgebraTransformer extends OpVisitorBase {
    private final Stack<Element> stack;

    public AlgebraTransformer()
    {
        stack = new Stack<Element>();
    }
    // triple assetions transform
    public  Element transform(Op op) {
         AlgebraWalker.walkBottomUp(this, op);
        return stack.pop();
    }
    @Override
    public void visit(OpBGP opBGP) {
        BasicPattern bp=opBGP.getPattern();
        ElementGroup elementGroup= new ElementGroup();
        ElementPathBlock elementPathBlock=new ElementPathBlock();
        for(Triple triple:bp.getList()){
            TriplePath triplePath=new TriplePath(triple);
            elementPathBlock.addTriple(triplePath);
        }
        elementGroup.addElement(elementPathBlock);
        stack.push(elementGroup);

    }
    @Override
    public void visit(OpFilter opFilter) {
        System.out.println("unsupported");
//        Op subOp = stack.pop();
//        stack.push(OpFilter.filterDirect(opFilter.getExprs(),subOp));
    }
    @Override
    public void visit(OpJoin opJoin) {
        Element rightElement = stack.pop();
        Element leftElement = stack.pop();
        ElementGroup elementGroup=new ElementGroup();
        elementGroup.addElement(rightElement);
        elementGroup.addElement(leftElement);
        stack.push(elementGroup);
//        Op rightOp = stack.pop();
//        Op leftOp = stack.pop();
//        stack.push(OpJoin.create(leftOp, rightOp));
    }
    @Override
    public void visit(OpSequence opSequence) {
        System.out.println("unsupported");
//        OpSequence Sequence = OpSequence.create();
//        for(int i=0; i<opSequence.size(); i++) {
//            Sequence.add(stack.pop());
//        }
//        stack.push(Sequence);

    }
    @Override
    public void visit(OpLeftJoin opLeftJoin) {
        System.out.println("unsupported");
//        Op rightOp = stack.pop();
//        Op leftOp = stack.pop();
//        stack.push(OpLeftJoin.create(leftOp, rightOp,  opLeftJoin.getExprs()));
    }

    @Override
    public void visit(OpConditional opConditional) {
        System.out.println("unsupported");
//        Op rightOp = stack.pop();
//        Op leftOp = stack.pop();
//        stack.push(new OpConditional(leftOp, rightOp));
    }

    @Override
    public void visit(OpUnion opUnion) {

        Element rightElement = stack.pop();
        Element leftElement = stack.pop();
        ElementUnion elementUnion=new ElementUnion(leftElement);
        elementUnion.addElement(rightElement);
        stack.push(elementUnion);


    }

    @Override
    public void visit(OpProject opProject) {
   //     System.out.println("cOp Project could be null");
//        Op subOp = stack.pop();
//        stack.push(new OpProject(subOp,opProject.getVars()));
    }

    @Override
    public void visit(OpDistinct opDistinct) {
        System.out.println("unsupported");
//        Op subOp = stack.pop();
//        stack.push(new OpDistinct(subOp));
    }

    @Override
    public void visit(OpOrder opOrder) {
        System.out.println("unsupported");
//        Op subOp = stack.pop();
//        stack.push(new OpOrder(subOp, opOrder.getConditions()));
    }

    @Override
    public void visit(OpSlice opSlice) {
        System.out.println("unsupported");
//        Op subOp = stack.pop();
//        stack.push(new OpSlice(subOp, opSlice.getStart(),opSlice.getLength()));
    }

    @Override
    public void visit(OpReduced opReduced) {
        System.out.println("unsupported");
//        Op subOp = stack.pop();
//        stack.push(OpReduced.create(subOp));
    }
}
