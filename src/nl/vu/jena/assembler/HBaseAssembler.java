package nl.vu.jena.assembler;

import org.apache.jena.assembler.Assembler;
import org.apache.jena.assembler.Mode;
import org.apache.jena.assembler.assemblers.AssemblerBase;
import org.apache.jena.graph.Graph;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import nl.vu.datalayer.hbase.HBaseClientSolution;
import nl.vu.datalayer.hbase.HBaseFactory;
import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;
import nl.vu.jena.graph.HBaseGraph;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class HBaseAssembler extends AssemblerBase {

	@Override
	public Model openModel( Resource root )
	{ 
		try {
			return createHBaseModel();

		} catch (IOException e) {
			e.printStackTrace();	
		}
		return null;
	}

	@Override
	public Object open(Assembler a, Resource root, Mode mode) {
		try {
			return createHBaseModel();

		} catch (IOException e) {
			e.printStackTrace();	
		}
		return null;
	}

	private Model createHBaseModel() throws IOException {
		HBaseConnection con;
		con = HBaseConnection.create(HBaseConnection.NATIVE_JAVA);

		HBaseClientSolution hbaseSol = HBaseFactory.getHBaseSolution(
				"local-" + HBPrefixMatchSchema.SCHEMA_NAME, con, null);

		Properties prop = new Properties();
		try{
			prop.load(new FileInputStream("config.properties"));
		}
		catch (IOException e) {
			//continue to use the default properties
		}
		String caching = prop.getProperty("engine_caching", "off");
		
		Graph g;
		if (caching.equals("on")){
			g = new HBaseGraph(hbaseSol, HBaseGraph.CACHING_ON);
		}
		else{
			g = new HBaseGraph(hbaseSol, HBaseGraph.CACHING_OFF);
		}
		
		return ModelFactory.createModelForGraph(g);
	}

	

}
