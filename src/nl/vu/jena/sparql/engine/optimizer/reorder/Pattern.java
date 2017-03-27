package nl.vu.jena.sparql.engine.optimizer.reorder;

import org.apache.jena.sparql.sse.Item;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.io.PrintUtils;
import org.apache.jena.atlas.io.Printable;

public class Pattern implements Printable {
	Item subjItem;
	Item predItem;
	Item objItem;
	double weight;
	boolean filtered = false;

	public Pattern(double w, Item subj, Item pred, Item obj) {
		weight = w;
		subjItem = subj;
		predItem = pred;
		objItem = obj;
	}
	
	public Pattern(double w, Item subj, Item pred, Item obj, boolean filtered) {
		weight = w;
		subjItem = subj;
		predItem = pred;
		objItem = obj;
		this.filtered = filtered;
	}

	@Override
	public String toString() {
		//return "("+subjItem+" "+predItem+" "+objItem+") ==> "+weight ;
		return PrintUtils.toString(this);
	}

	@Override
	public void output(IndentedWriter out) {
		out.print("(");
		out.print("(");
		out.print(subjItem.toString());
		out.print(" ");
		out.print(predItem.toString());
		out.print(" ");
		out.print(objItem.toString());
		out.print(")");
		out.print(" ");
		out.print(Double.toString(weight));
		out.print(")");
	}
}
