package edu.umass.cs.pig.test;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
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

public class TestExGen4FilterUDF {
	static PigContext pigContext = new PigContext(ExecType.LOCAL, new Properties());

//  private OSValidator z3 = OSValidator.get();
//  private Z3Context z4 = Z3Context.get();
  static int MAX = 100;
  static int MIN = 10;
  static String A, B, C, D, E, U, I;
  static int start = 0;
  static  File fileA, fileB, fileC, fileD, fileE, fileU, fileI;
  
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
      fileB = File.createTempFile("dataB", ".dat");
      fileC = File.createTempFile("dataC", ".dat");
      fileD = File.createTempFile("dataD", ".dat");
      fileE = File.createTempFile("dataE", ".dat");
      fileU = File.createTempFile("dataU", ".dat");
      fileI = File.createTempFile("dataI", ".dat");

      writeData(fileA);
      writeData(fileB);
      writeData2(fileC);
      writeData3(fileD);
      writeData3(fileE);
      writeData6(fileI);
      writeData17(fileU);

      fileA.deleteOnExit();
      fileB.deleteOnExit();
      fileC.deleteOnExit();
      fileD.deleteOnExit();
      fileE.deleteOnExit();
      fileI.deleteOnExit();
      fileU.deleteOnExit();
      
      A = "'" + fileA.getPath() + "'";
      B = "'" + fileB.getPath() + "'";
      C = "'" + fileC.getPath() + "'";
      D = "'" + fileD.getPath() + "'";
      E = "'" + fileE.getPath() + "'";
      I = "'" + fileI.getPath() + "'";
      U = "'" + fileU.getPath() + "'";
      System.out.println("A : " + A + "\n" + "B : " + B + "\n" + "C : " + C + "\n" + "D : " + D + "\n" + "E : " + E + "\n" + "I : " + I + "\n" + "U : " + U + "\n");
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
  
  private static void writeData2(File dataFile) throws Exception {
      // File dataFile = File.createTempFile(name, ".dat");
      FileOutputStream dat = new FileOutputStream(dataFile);

      Random rand = new Random();

      for (int i = 0; i < MIN; i++)
          dat.write((rand.nextInt(100) + "\t" + rand.nextInt(100) + "\n")
                  .getBytes());

      dat.close();
  }
  
  private static void writeData3(File dataFile) throws Exception {
      // File dataFile = File.createTempFile(name, ".dat");
	  start = 0;
      FileOutputStream dat = new FileOutputStream(dataFile);

      for (int i = 0; i < MIN; i++)
          dat.write((start++ + "\t" + start++ + "\n")
                  .getBytes());

      dat.close();
  }
  
  private static void writeData6(File dataFile) throws Exception {
	  FileOutputStream dat = new FileOutputStream(dataFile);

      Random rand = new Random();

      for (int i = 0; i < MIN; i++)
          dat.write(( rand.nextDouble() + "\t" + 10*rand.nextDouble() + "\n")
                  .getBytes());

      dat.close();
  }
  
  private static void writeData17(File dataFile) throws Exception {
	  FileOutputStream dat = new FileOutputStream(dataFile);

      Random rand = new Random();

      for (int i = 0; i < MIN; i++)
    	  dat.write(( rand.nextDouble() + "\t" + rand.nextDouble() + "\t" + rand.nextDouble() + "\n")
                  .getBytes());

          
      dat.close();
  }
  
  @Test
  public void testFilterWithIsNull() throws ExecException, IOException {
      PigServer pigServer = new PigServer(pigContext);

      pigServer.registerQuery("A = load " + A
              + " using PigStorage() as (x : int, y : int);");
      pigServer.registerQuery("B = filter A by x is not null AND x > 10;");

      Map<Operator, DataBag> derivedData = pigServer.getExamples2("B");

      assertTrue(derivedData != null);
  }
  
  @Test
  public void testFilterWithUDF() throws ExecException, IOException {
      PigServer pigServer = new PigServer(pigContext);

      pigServer.registerQuery("A = load " + A
              + " using PigStorage() as (x : int, y : int);");
      pigServer.registerQuery("B = group A by x;");
      pigServer.registerQuery("C = filter B by NOT IsEmpty(A.y);");

      Map<Operator, DataBag> derivedData = pigServer.getExamples2("C");

      assertTrue(derivedData != null);
  }
  
  @Test
  public void testFilterGroupCountStore() throws Exception {
      File out = File.createTempFile("testFilterGroupCountStoreOutput", "");
      out.deleteOnExit();
      out.delete();
  
      PigServer pigServer = new PigServer(pigContext);
      pigServer.setBatchOn();
      pigServer.registerQuery("A = load " + A.toString() + " as (x, y);");
      pigServer.registerQuery("B = filter A by x < 5;");
      pigServer.registerQuery("C = group B by x;");
      pigServer.registerQuery("D = foreach C generate group as x, COUNT(B) as the_count;");
      pigServer.registerQuery("store D into '" +  out.getAbsolutePath() + "';");
      Map<Operator, DataBag> derivedData = pigServer.getExamples2(null);
  
      assertTrue(derivedData != null);
  }
  
  @Test
  public void testFilterWithUDF2() throws Exception {

  	PigServer pigserver = new PigServer(pigContext);

      String query = "A = load " + U
              + " using PigStorage() as (x : double, y : double, z : double);\n";
      pigserver.registerQuery(query);
      System.out.print(query);
      query = "B = filter A by org.apache.pig.piggybank.evaluation.math.POW(x, y) > 3.0 + z;\n";
      pigserver.registerQuery(query);
      System.out.print(query);
      Map<Operator, DataBag> derivedData = pigserver.getExamples2("B");
      assertTrue(derivedData != null);
//      float yy = new org.apache.pig.piggybank.evaluation.math.POW();
  }// 
  
  @Test
  public void testFilterWithUDF3() throws Exception {

  	PigServer pigserver = new PigServer(pigContext);

      String query = "A = load " + I
              + " using PigStorage() as (x : double, y : double);\n";
      pigserver.registerQuery(query);
      System.out.print(query);
      query = "B = filter A by org.apache.pig.piggybank.evaluation.math.POW(x, y) > 3.0;\n";
      pigserver.registerQuery(query);
      System.out.print(query);
      Map<Operator, DataBag> derivedData = pigserver.getExamples2("B");
      assertTrue(derivedData != null);
//      float yy = new org.apache.pig.piggybank.evaluation.math.POW();
  }// 

}