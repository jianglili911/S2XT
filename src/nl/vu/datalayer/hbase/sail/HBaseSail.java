package nl.vu.datalayer.hbase.sail;

import nl.vu.datalayer.hbase.HBaseClientSolution;
import nl.vu.datalayer.hbase.HBaseFactory;
import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.NotifyingSailBase;

import java.io.IOException;

/**
 * An implementation of a Sail interface, that retrieves data from an HBase store. 
 * 
 * @author Anca Dumitrache, Antonis Loizou
 */
public class HBaseSail extends NotifyingSailBase {

	private HBaseClientSolution hbase;
	private HBaseConnection con;
	
	private ValueFactory valueFactory = new ValueFactoryImpl();
	
	/**
	 * Establishes a connection to the HBase store, via the {@link HBaseConnection} and
	 * {@link HBaseFactory} classes.
	 */
	public HBaseSail() {
		try {
			HBaseConnection con = HBaseConnection.create(HBaseConnection.NATIVE_JAVA);
			hbase = HBaseFactory.getHBaseSolution(HBPrefixMatchSchema.SCHEMA_NAME, con, null);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public ValueFactory getValueFactory() {
		return valueFactory;
	}
	
	HBaseClientSolution getHBase() {
		return hbase;
	}

	/**
	 * Returns a store-specific {@link HBaseConnection} object.
	 * @return A connection to the store.
	 */
	HBaseConnection getHBaseConnection() {
		return con;
	}

	@Override
	public boolean isWritable() throws SailException {
		return true;
	}

	@Override
	protected NotifyingSailConnection getConnectionInternal()
			throws SailException {
		return new HBaseSailConnection(this);
	}

	@Override
	protected void shutDownInternal() throws SailException {
		try {
			con.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
