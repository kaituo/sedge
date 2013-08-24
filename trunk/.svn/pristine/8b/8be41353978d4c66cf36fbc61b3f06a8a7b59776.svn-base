package edu.umass.cs.pig.test;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.newplan.Operator;
import org.apache.pig.test.MiniCluster;
import org.apache.pig.test.SessionIdentifierGenerator;
import org.apache.pig.test.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestExGen4Join {
	static PigContext pigContext = new PigContext(ExecType.LOCAL, new Properties());
	private final Log log = LogFactory.getLog(getClass());
	
//  private OSValidator z3 = OSValidator.get();
//  private Z3Context z4 = Z3Context.get();
  static int MAX = 100;
  static int MIN = 10;
  static String A, B, C, D, E, F, G;
  static int start = 0;
  static  File fileA, fileB, fileC, fileD, fileE, fileF, fileG;
  
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
      fileF = File.createTempFile("dataF", ".dat");
      fileG = File.createTempFile("dataG", ".dat");

      writeData(fileA);
      writeData(fileB);
      writeData2(fileC);
      writeData3(fileD);
      writeData3(fileE);
      writeData4(fileF);
      writeData5(fileG);

      fileA.deleteOnExit();
      fileB.deleteOnExit();
      fileC.deleteOnExit();
      fileD.deleteOnExit();
      fileE.deleteOnExit();
      fileF.deleteOnExit();
      fileG.deleteOnExit();
      
      A = "'" + fileA.getPath() + "'";
      B = "'" + fileB.getPath() + "'";
      C = "'" + fileC.getPath() + "'";
      D = "'" + fileD.getPath() + "'";
      E = "'" + fileE.getPath() + "'";
      F = "'" + fileF.getPath() + "'";
      G = "'" + fileG.getPath() + "'";
      System.out.println("A : " + A + "\n" + "B : " + B + "\n" + "C : " + C +  "\n" + "D : " + D +  "\n" + "E : " + E +  "\n");
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
      FileOutputStream dat = new FileOutputStream(dataFile);

      for (int i = 0; i < MIN; i++)
          dat.write((start++ + "\t" + start++ + "\n")
                  .getBytes());

      dat.close();
  }
  
  private static void writeData4(File dataFile) throws Exception {
      // File dataFile = File.createTempFile(name, ".dat");
      FileOutputStream dat = new FileOutputStream(dataFile);

      for (int i = 0; i < MIN; i++)
          dat.write(("ba" + "\t" + start++ + "\n")
                  .getBytes());

      dat.close();
  }
  
  private static void writeData5(File dataFile) throws Exception {
      // File dataFile = File.createTempFile(name, ".dat");
      FileOutputStream dat = new FileOutputStream(dataFile);

      for (int i = 0; i < MIN; i++)
          dat.write(("ab" + "\t" + start++ + "\n")
                  .getBytes());

      dat.close();
  }
  
	
  @Test
  public void testJoin() throws IOException, ExecException {
      PigServer pigServer = new PigServer(pigContext);
      pigServer.registerQuery("A1 = load " + A + " using PigStorage() as (x:int, y:int);");
      pigServer.registerQuery("B1 = load " + B + " using PigStorage() as (x:int, y:int);");

      pigServer.registerQuery("E = join A1 by x, B1 by x;");

      Map<Operator, DataBag> derivedData = pigServer.getExamples2("E");

      assertTrue(derivedData != null);
  }

  @Test
  public void testJoin2() throws IOException, ExecException {
      PigServer pigServer = new PigServer(pigContext);
      pigServer.registerQuery("A1 = load " + A + " using PigStorage() as (x:int, y:int);");
      pigServer.registerQuery("B1 = load " + A + " using PigStorage() as (x:int, y:int);");

      pigServer.registerQuery("E = join A1 by x, B1 by x;");

      Map<Operator, DataBag> derivedData = pigServer.getExamples2("E");

      assertTrue(derivedData != null);
  }
  
  @Test
  public void testJoin3() throws IOException, ExecException {
      PigServer pigServer = new PigServer(pigContext);
      pigServer.registerQuery("A1 = load " + C + " using PigStorage() as (x:int, y:int);");
      pigServer.registerQuery("B1 = load " + A + " using PigStorage() as (x:int, y:int);");

      pigServer.registerQuery("E = join A1 by x, B1 by x;");

      Map<Operator, DataBag> derivedData = pigServer.getExamples2("E");

      assertTrue(derivedData != null);
  }
  
  @Test
  public void testJoin4() throws IOException, ExecException {
      PigServer pigServer = new PigServer(pigContext);
      pigServer.setBatchOn();
      
      pigServer.registerQuery("A1 = load " + C + " using PigStorage() as (x:int, y:int);");
      pigServer.registerQuery("B1 = load " + A + " using PigStorage() as (x:int, y:int);");

      pigServer.registerQuery("B = join A1 by x, B1 by x;");
      pigServer.registerQuery("D = foreach B generate A1::x as a1x, A1::y as a1y, B1::y as b1y;");

      Map<Operator, DataBag> derivedData = pigServer.getExamples2("D");

      assertTrue(derivedData != null);
  }
  
  @Test
  public void testJoin5() throws IOException, ExecException {
	  File out = File.createTempFile("testFilterGroupCountStoreOutput", "");
      out.deleteOnExit();
      out.delete();
      
      PigServer pigServer = new PigServer(pigContext);
      pigServer.setBatchOn();
      
      pigServer.registerQuery("A1 = load " + C + " using PigStorage() as (x:int, y:int);");
      pigServer.registerQuery("B1 = load " + A + " using PigStorage() as (x:int, y:int);");

      pigServer.registerQuery("B = join A1 by (x, y), B1 by (x, y);");
      
      pigServer.registerQuery("store B into '" +  out.getAbsolutePath() + "';");

      Map<Operator, DataBag> derivedData = pigServer.getExamples2(null);

      assertTrue(derivedData != null);
  }
  
    /*bug in illustrate
     * Would report the following error trace:
     *      org.apache.pig.backend.executionengine.ExecException: ERROR 1071: Cannot convert a tuple to an Integer
			at org.apache.pig.data.DataType.toInteger(DataType.java:582)
			at org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POCast.getNext(POCast.java:150)
			at org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator.getNext(PhysicalOperator.java:328)
			at org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach.processPlan(POForEach.java:332)
			at org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach.getNext(POForEach.java:284)
			at org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator.processInput(PhysicalOperator.java:290)
			at org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange.getNext(POLocalRearrange.java:256)
			at org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion.getNext(POUnion.java:165)
			at org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapBase.runPipeline(PigMapBase.java:261)
			at org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapBase.map(PigMapBase.java:256)
			at org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapBase.map(PigMapBase.java:58)
			at org.apache.hadoop.mapreduce.Mapper.run(Mapper.java:144)
			at org.apache.pig.pen.LocalMapReduceSimulator.launchPig(LocalMapReduceSimulator.java:205)
			at org.apache.pig.pen.ExampleGenerator.getData(ExampleGenerator.java:257)
			at org.apache.pig.pen.ExampleGenerator.getData(ExampleGenerator.java:238)
			at org.apache.pig.pen.LineageTrimmingVisitor.init(LineageTrimmingVisitor.java:103)
			at org.apache.pig.pen.LineageTrimmingVisitor.<init>(LineageTrimmingVisitor.java:98)
			at org.apache.pig.pen.ExampleGenerator.getExamples(ExampleGenerator.java:185)
			at org.apache.pig.PigServer.getExamples(PigServer.java:1258)
			at org.apache.pig.tools.grunt.GruntParser.processIllustrate(GruntParser.java:698)
			at org.apache.pig.tools.pigscript.parser.PigScriptParser.Illustrate(PigScriptParser.java:591)
			at org.apache.pig.tools.pigscript.parser.PigScriptParser.parse(PigScriptParser.java:306)
			at org.apache.pig.tools.grunt.GruntParser.parseStopOnError(GruntParser.java:188)
			at org.apache.pig.tools.grunt.GruntParser.parseStopOnError(GruntParser.java:164)
			at org.apache.pig.tools.grunt.Grunt.run(Grunt.java:67)
			at org.apache.pig.Main.run(Main.java:487)
			at org.apache.pig.Main.main(Main.java:108)
    2012-04-25 15:18:49,609 [main] ERROR org.apache.pig.tools.grunt.Grunt - ERROR 2997: Encountered IOException. ExecException : Cannot convert a tuple to an Integer
    Details at logfile: /nfs/ktl/home1/kaituo/pig_1335381455900.log
     */
//  @Test
//  public void testJoin6() throws IOException, ExecException {
//	  File out = File.createTempFile("testFilterGroupCountStoreOutput", "");
//      out.deleteOnExit();
//      out.delete();
//      
//      PigServer pigServer = new PigServer(pigContext);
//      pigServer.setBatchOn();
//      
//      pigServer.registerQuery("A1 = load " + C + " as (x:int, y:int);");
//      pigServer.registerQuery("B1 = load " + A + " as (x:int, w:int);");
//
//      pigServer.registerQuery("B = join A1 by x, B1 by x;");
//      
//      pigServer.registerQuery("C = filter B by A1::x < B1::w;"); 
//
//      Map<Operator, DataBag> derivedData = pigServer.getExamples("C");
//
//      assertTrue(derivedData != null);
//  }
  
  @Test
  public void testJoin7() throws IOException, ExecException {
	  File out = File.createTempFile("testFilterGroupCountStoreOutput", "");
      out.deleteOnExit();
      out.delete();
      
      PigServer pigServer = new PigServer(pigContext);
      pigServer.setBatchOn();
      
      pigServer.registerQuery("A1 = load " + D + " using PigStorage() as (x:int, y:int);");
      pigServer.registerQuery("B1 = load " + E + " using PigStorage() as (x:int, y:int);");

      pigServer.registerQuery("B = join A1 by (x, y), B1 by (x, y);");
      
      pigServer.registerQuery("store B into '" +  out.getAbsolutePath() + "';");

      Map<Operator, DataBag> derivedData = pigServer.getExamples2(null);

      assertTrue(derivedData != null);
  }
  
  @Test
  public void testJoin8() throws IOException, ExecException {
	  
      
      PigServer pigServer = new PigServer(pigContext);
      pigServer.setBatchOn();
      
      pigServer.registerQuery("A1 = load " + D + " using PigStorage() as (x:int, y:int);");
      pigServer.registerQuery("B1 = load " + E + " using PigStorage() as (x:int, w:int);");

      pigServer.registerQuery("B = join A1 by x, B1 by x;");
      
      //pigServer.registerQuery("store B into '" +  out.getAbsolutePath() + "';");

      Map<Operator, DataBag> derivedData = pigServer.getExamples2("B");

      assertTrue(derivedData != null);
  }
  
  @Test
  public void testJoin9() throws IOException, ExecException {
      PigServer pigServer = new PigServer(pigContext);

      pigServer.registerQuery("A=load " 
              + F + " using PigStorage() as (x : chararray, y : int);");
      pigServer.registerQuery("C=load " 
              + G + " using PigStorage() as (x : chararray, y : int);");
      String query = "B = join A by x, C by x;";

      log.info(query);
      pigServer.registerQuery(query);
      Map<Operator, DataBag> derivedData = pigServer.getExamples2("B");

      assertTrue(derivedData != null);
  }
  
  @Test
  public void testJoin10() throws IOException, ExecException {
      PigServer pigServer = new PigServer(pigContext);
      pigServer.registerQuery("A1 = load " + A + " using PigStorage() as (x:int, y:int);");
      pigServer.registerQuery("B1 = load " + B + " using PigStorage() as (x:int, y:int);");

      pigServer.registerQuery("C = join A1 by x, B1 by x;");
      pigServer.registerQuery("E = distinct C;");
      Map<Operator, DataBag> derivedData = pigServer.getExamples2("E");

      assertTrue(derivedData != null);
  }

}
