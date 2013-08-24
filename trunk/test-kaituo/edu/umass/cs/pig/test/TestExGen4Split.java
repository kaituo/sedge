package edu.umass.cs.pig.test;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.Operator;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestExGen4Split {
	static PigContext pigContext = new PigContext(ExecType.LOCAL, new Properties());
	static int MAX = 100;
	static int MIN = 10;
	static String A, L;
	static int start = 0;
	static  File fileA, fileL;
	
	{
		try {
			pigContext.connect();
		} catch (ExecException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@BeforeClass
	  public static void oneTimeSetup() throws Exception {
	      fileA = File.createTempFile("dataA", ".dat");
	      fileL = File.createTempFile("dataL", ".dat");
	      
	      writeData(fileA);
	      writeData9(fileL);
	      
	      fileA.deleteOnExit();
	      fileL.deleteOnExit();
	      
	      A = "'" + fileA.getPath() + "'";
	      L = "'" + fileL.getPath() + "'";
	      
	      System.out.println("A : " + A + "\n" + "L : " + L + "\n");
	      System.out.println("Test data created.");
	}
	
	private static void writeData(File dataFile) throws Exception {
	      // File dataFile = File.createTempFile(name, ".dat");
	      FileOutputStream dat = new FileOutputStream(dataFile);

	      Random rand = new Random();

	      for (int i = 0; i < MIN; i++)
	          dat.write((rand.nextInt(10) + "\t" + rand.nextInt(10) + "\n")
	                  .getBytes());

	      dat.close();
	  }
	
	private static void writeData9(File dataFile) throws Exception {
		  FileOutputStream dat = new FileOutputStream(dataFile);

	      for (int i = 0; i < MIN; i++)
	          dat.write(( UUID.randomUUID().toString() + "\t" + UUID.randomUUID().toString() + "\n")
	                  .getBytes());

	      dat.close();
	  }
	
	@Test
	  public void testSplit1() throws Exception {
		  File out = File.createTempFile("testFilterGroupCountStoreOutput", "");
	      out.deleteOnExit();
	      out.delete();
	  
	      PigServer pigServer = new PigServer(pigContext);
	      pigServer.setBatchOn();
	      pigServer.registerQuery("A = load " + A.toString() + " using PigStorage() as (id1: int, id2: int);");
	      pigServer.registerQuery("split A into B if id1 > 3, C if id1 < 3, D otherwise;");
	      
	      Map<Operator, DataBag> derivedData = pigServer.getExamples2("B");
	  
	      assertTrue(derivedData != null);
	  }
	
	@Test
	  public void testSplit2() throws Exception {
		  File out = File.createTempFile("testFilterGroupCountStoreOutput", "");
	      out.deleteOnExit();
	      out.delete();
	  
	      PigServer pigServer = new PigServer(pigContext);
	      pigServer.setBatchOn();
	      pigServer.registerQuery("A = load " + A.toString() + " using PigStorage() as (id1: int, id2: int);");
	      pigServer.registerQuery("split A into B if id1 > 10, C if id1 < 10, D otherwise;");
	      
	      Map<Operator, DataBag> derivedData = pigServer.getExamples2("B");
	  
	      assertTrue(derivedData != null);
	  }
	
//	split B into C if user is not null, alpha if user is null;
//	split C into D if query_term is not null, aleph if query_term is null;
	@Test
	  public void testSplit3() throws Exception {
		  File out = File.createTempFile("testFilterGroupCountStoreOutput", "");
	      out.deleteOnExit();
	      out.delete();
	  
	      PigServer pigServer = new PigServer(pigContext);
	      pigServer.setBatchOn();
	      pigServer.registerQuery("A = load " + L.toString() + " using PigStorage() as (user: chararray, query_term: chararray);");
	      pigServer.registerQuery("split A into B if user is not null, alpha if user is null;");
	      pigServer.registerQuery("split B into C if query_term is not null, aleph if query_term is null;");
	      
	      Map<Operator, DataBag> derivedData = pigServer.getExamples2("alpha");
	  
	      assertTrue(derivedData != null);
	  }

}
