package edu.umass.cs.pig.test;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.Operator;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestExGen4FilterFloat {
	static PigContext pigContext = new PigContext(ExecType.LOCAL, new Properties());

	static String A;
	static  File fileA;
	static int MAX = 100;
	static int MIN = 10;
	
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

	      writeData(fileA);
	      
	      fileA.deleteOnExit();
	      
	      A = "'" + fileA.getPath() + "'";
	      System.out.println("A : " + A + "\n");
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
	
	@Test
	  public void testFloat1() throws Exception {
		  PigServer pigserver = new PigServer(pigContext);
		  String query = "A = load " + A
	              + " using PigStorage() as (x : float, y : float);\n";
	      pigserver.registerQuery(query);
	      query = "B = filter A by x > 0.5;";
	      pigserver.registerQuery(query);
	      Map<Operator, DataBag> derivedData = pigserver.getExamples("B");
	      
	      assertTrue(derivedData != null);
	  }
	
	@Test
	  public void testFloat2() throws Exception {
		  PigServer pigserver = new PigServer(pigContext);
		  String query = "A = load " + A
	              + " using PigStorage() as (x : float, y : float);\n";
	      pigserver.registerQuery(query);
	      query = "B = filter A by x > 0.5 and x<0.6;";
	      pigserver.registerQuery(query);
	      Map<Operator, DataBag> derivedData = pigserver.getExamples("B");
	      
	      assertTrue(derivedData != null);
	  }

}
