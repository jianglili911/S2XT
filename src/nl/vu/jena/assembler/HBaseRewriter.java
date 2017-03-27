package nl.vu.jena.assembler;

import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.engine.main.StageBuilder;
import nl.vu.jena.sparql.engine.main.HBaseStageGenerator;
import nl.vu.jena.sparql.engine.optimizer.HBaseOptimize;
import nl.vu.jena.sparql.engine.optimizer.HBaseTransformFilterPlacement;

public class HBaseRewriter {
static {
	HBaseStageGenerator hbaseStageGenerator = new HBaseStageGenerator();
	StageBuilder.setGenerator(ARQ.getContext(), hbaseStageGenerator) ;
	
	ARQ.getContext().set(ARQConstants.sysOptimizerFactory, HBaseOptimize.hbaseOptimizationFactory);
	ARQ.getContext().set(ARQ.optFilterPlacement, new HBaseTransformFilterPlacement());
}
}
