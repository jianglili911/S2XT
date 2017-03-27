package nl.vu.datalayer.hbase.coprocessor;

import org.apache.hadoop.hbase.coprocessor.CoprocessorService;

import java.io.IOException;

//import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
//import org.apache.hadoop.hbase.Coprocessor;
//import org.apache.hadoop.hbase.coprocessor.example.BulkDeleteEndpoint;


public interface PrefixMatchProtocol extends /*Coprocessor,*/ CoprocessorService{



	  public void generateSecondaryIndex() throws IOException;


	public void stopGeneration() throws IOException;



}
