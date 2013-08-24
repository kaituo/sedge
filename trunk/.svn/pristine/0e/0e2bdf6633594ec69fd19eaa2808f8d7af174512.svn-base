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

public class TestExGen4SDSS {
	static PigContext pigContext = new PigContext(ExecType.LOCAL, new Properties());

//  private OSValidator z3 = OSValidator.get();
//  private Z3Context z4 = Z3Context.get();
  static int MAX = 100;
  static int MIN = 10;
  static String A, F, H, I, V, W, Y, Z;
  static int start = 0;
  static  File fileA, fileF, fileH, fileI, fileV, fileW, fileY, fileZ;
  
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
      fileF = File.createTempFile("dataF", ".dat");
      fileH = File.createTempFile("dataH", ".dat");
      fileI = File.createTempFile("dataI", ".dat");
      fileV = File.createTempFile("dataV", ".dat");
      fileW = File.createTempFile("dataW", ".dat");
      fileY = File.createTempFile("dataY", ".dat");
      fileZ = File.createTempFile("dataZ", ".dat");
      
      writeData(fileA);
      writeData4(fileF);
      writeData5(fileH);
      writeData6(fileI);
      writeData18(fileV);
      writeData19(fileW);
      writeData21(fileY);
      writeData22(fileZ);


      fileA.deleteOnExit();
      fileF.deleteOnExit();
      fileH.deleteOnExit();
      fileI.deleteOnExit();
      fileV.deleteOnExit();
      fileW.deleteOnExit();
      fileY.deleteOnExit();
      fileZ.deleteOnExit();
      
      
      A = "'" + fileA.getPath() + "'";
      F = "'" + fileF.getPath() + "'";
      H = "'" + fileH.getPath() + "'";
      I = "'" + fileI.getPath() + "'";
      V = "'" + fileV.getPath() + "'";
      W = "'" + fileW.getPath() + "'";
      Y = "'" + fileY.getPath() + "'";
      Z = "'" + fileZ.getPath() + "'";
   
      System.out.println("A : " + A + "\n" + "F : " + F + "\n" + "H : " + H + "\n" + "I : " + I + "\n" + "V : " + V + "\n" + "W : " + W + "\n" + "Y : " + Y + "\n" + "Z : " + Z + "\n");
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
	
	private static void writeData4(File dataFile) throws Exception {
		  FileOutputStream dat = new FileOutputStream(dataFile);

	      Random rand = new Random();

	      for (int i = 0; i < MIN; i++)
	          dat.write(( UUID.randomUUID().toString() + "\t" + rand.nextInt(10) + "\t" + rand.nextInt(10) + "\n")
	                  .getBytes());

	      
//	      dat.write(( "John" + "\t" + -100 + "\t" + -100 + "\n")
//	              .getBytes());
//	      dat.write(( "John" + "\t" + 2 + "\t" + 2 + "\n")
//	              .getBytes());
//	      dat.write(( "John" + "\t" + 100 + "\t" + 100 + "\n")
//	              .getBytes());

	      dat.close();
	  }
	
	private static void writeData5(File dataFile) throws Exception {
		  FileOutputStream dat = new FileOutputStream(dataFile);

	      Random rand = new Random();

	      for (int i = 0; i < MIN; i++)
	          dat.write(( rand.nextFloat() + "\t" + 10*rand.nextFloat() + "\n")
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
	
	
	
	private static void writeData18(File dataFile) throws Exception {
		  FileOutputStream dat = new FileOutputStream(dataFile);

	      Random rand = new Random();

	      for (int i = 0; i < MIN; i++)
	          dat.write(( rand.nextFloat() + "\t" + rand.nextFloat() + "\t" + rand.nextFloat() + "\t" + rand.nextFloat() + "\n")
	                  .getBytes());

	      dat.close();
	}
	
	private static void writeData19(File dataFile) throws Exception {
		  FileOutputStream dat = new FileOutputStream(dataFile);

	      Random rand = new Random();

	      for (int i = 0; i < MIN; i++)
	    	  dat.write(( rand.nextDouble() + "\t" + rand.nextDouble() + "\t" + rand.nextDouble() + "\t" + rand.nextDouble() + "\t" + rand.nextDouble() + "\n")
	                  .getBytes());

	          
	      dat.close();
	  }
	
	
	private static void writeData21(File dataFile) throws Exception {
		  FileOutputStream dat = new FileOutputStream(dataFile);

	      Random rand = new Random();

	      for (int i = 0; i < MIN; i++)
	          dat.write(( 6*rand.nextDouble() + "\t" + 5*rand.nextDouble() + "\n")
	                  .getBytes());

	      dat.close();
	  }
	
	private static void writeData22(File dataFile) throws Exception {
		  FileOutputStream dat = new FileOutputStream(dataFile);

	      Random rand = new Random();

	      for (int i = 0; i < MIN; i++)
	          dat.write(( rand.nextLong() + "\t" + rand.nextInt() + "\n")
	                  .getBytes());

	      dat.close();
	  }
	
	@Test
	  public void testBasic() throws Exception {

		  PigServer pigServer = new PigServer(pigContext);

	      pigServer.registerQuery("A = load " + A
	              + " using PigStorage() as (run : int, field : int);");
	      pigServer.registerQuery("B = filter A by run == 1336 and field ==11 ;");
	      Map<Operator, DataBag> derivedData = pigServer.getExamples("B");

	      assertTrue(derivedData != null);
	  }//
	
	@Test
	  public void testGalaxies2Criteria() throws Exception {

		  PigServer pigServer = new PigServer(pigContext);

	      pigServer.registerQuery("A = load " + I
	              + " using PigStorage() as (r : double, extinction_r : double);");
	      pigServer.registerQuery("B = filter A by r < 22.0 and  r > 0.175 ;");
	      Map<Operator, DataBag> derivedData = pigServer.getExamples("B");

	      assertTrue(derivedData != null);
	  }// 
	
	/**
	 * NB:  Chris's implementation would generate runtime exception when generate example data 
	 * for the following Pig Scripts. 
	 * 
	   * SELECT specObjID
		FROM SpecObj
		WHERE SpecClass = dbo.fSpecClass('UNKNOWN') 
		
		    --  
	    CREATE FUNCTION fSpecClass(@name varchar(40))  
	    -------------------------------------------------------------------------------  
	    --/H Returns the SpecClass value, indexed by name  
	    -------------------------------------------------------------------------------  
	    --/T the  SpecClass values can be found with   
	    --/T <br>       Select * from SpecClass   
	    --/T <br>  
	    --/T Sample call to fSpecClass.  
	    --/T <samp>   
	    --/T <br> select top 10  *  
	    --/T <br> from SpecObj  
	    --/T <br> where specClass = dbo.fSpecClass('QSO')  
	    --/T </samp>   
	    --/T <br> see also fSpecClassN  
	    -------------------------------------------------------------  
	    RETURNS INTEGER  
	    AS BEGIN  
	    RETURN ( SELECT value   
	        FROM SpecClass  
	        WHERE name = UPPER(@name)  
	        )  
	    END  
	   * @throws Exception
	   */
//	  @Test
//	  public void testUnclassifiedSpectra() throws Exception {
//
//		  PigServer pigServer = new PigServer(pigContext);
//
//		  pigServer.registerQuery("A = load " + F
//	              + " using PigStorage() as (name : chararray, value : int, colv : int);\n");
//	      pigServer.registerQuery("B = filter A by name eq 'UNKNOWN';\n");
//	      pigServer.registerQuery("C = load " + Z
//	              + " using PigStorage() as (specObjID : long, SpecClass : int);\n");
//	     pigServer.registerQuery("D = join B by value, C by SpecClass;\n");
//	     pigServer.registerQuery("E = foreach D generate specObjID;\n");
//	      
//	      Map<Operator, DataBag> derivedData = pigServer.getExamples2("E");
//
//	      assertTrue(derivedData != null);
//	  }// 
	
	
	
	@Test
	public void testMultipleCriteria() throws Exception {
		  PigServer pigServer = new PigServer(pigContext);

	      pigServer.registerQuery("A = load " + V
	              + " using PigStorage() as (ra : float, dec : float, g : float, rho : float);");
	      pigServer.registerQuery("B = filter A by ra <= 270.0 and  ra >= 250.0 and dec > 50.0 and g+rho >= 23.0 and g+rho <= 25.0 ;");
	      Map<Operator, DataBag> derivedData = pigServer.getExamples("B");

	      assertTrue(derivedData != null);
	}
	
	@Test
	public void testSpatialUnitVectors() throws Exception {
		  PigServer pigServer = new PigServer(pigContext);

	      pigServer.registerQuery("A = load " + H
	              + " using PigStorage() as (cx : float, cy : float);");
	      pigServer.registerQuery("B = filter A by  (-0.642788 * cx + 0.766044 * cy>=0.0) and (-0.984808 * cx - 0.173648 * cy <0.0) ;");
	      Map<Operator, DataBag> derivedData = pigServer.getExamples("B");

	      assertTrue(derivedData != null);
	}
	
	
	
	@Test
	public void testCVs() throws Exception {
		PigServer pigServer = new PigServer(pigContext);

	    pigServer.registerQuery("A = load " + W
	              + " using PigStorage() as (u : double, g : double, r : double, i : double, z : double);");
	    pigServer.registerQuery("B = filter A by  u - g < 0.4 and g - r < 0.7 and r - i > 0.4 and i - z > 0.4 ;");
	    Map<Operator, DataBag> derivedData = pigServer.getExamples("B");

	    assertTrue(derivedData != null);
	}
	
	@Test
	public void testLowzQSOs() throws Exception {
		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("A = load " + W
	              + " using PigStorage() as (u : double, g : double, r : double, i : double, z : double);");
	    pigServer.registerQuery("B = filter A by   ( (g <= 22) and (u - g >= -0.27) and (u - g < 0.71) and (g - r >= -0.24) and (g - r < 0.35) and (r - i >= -0.27) and (r - i < 0.57) and (i - z >= -0.35) and (i - z < 0.70) )  ;");
	    Map<Operator, DataBag> derivedData = pigServer.getExamples("B");

	    assertTrue(derivedData != null);
	}
	
	@Test
	public void testVelocitiesErrors() throws Exception {
		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("A = load " + W
	              + " using PigStorage() as (rowv : double, rowvErr : double, colv : double, i : double, colvErr : double);");
	    pigServer.registerQuery("B = filter A by org.apache.pig.piggybank.evaluation.math.POW(rowv, 2.0) / org.apache.pig.piggybank.evaluation.math.POW(rowvErr, 2.0) + org.apache.pig.piggybank.evaluation.math.POW(colv, 2.0) / org.apache.pig.piggybank.evaluation.math.POW(colvErr, 2.0) > 4.0 ;");
	    Map<Operator, DataBag> derivedData = pigServer.getExamples("B");

	    assertTrue(derivedData != null);
	}
	
	@Test
	public void testBETWEEN() throws Exception {
		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("A = load " + W
	              + " using PigStorage() as (r : double, rho : double, isoA_r : double, q_r : double, u_r : double);");
	    pigServer.registerQuery("B = filter A by r + rho < 24.0 and  isoA_r >= 75.0 and isoA_r <= 150.0 and  (org.apache.pig.piggybank.evaluation.math.POW(q_r,2.0) + org.apache.pig.piggybank.evaluation.math.POW(u_r,2.0)) > 0.25;");
	    Map<Operator, DataBag> derivedData = pigServer.getExamples("B");

	    assertTrue(derivedData != null);
	}
	
	
	/**
	 * getExamples2: success 8; failure 2;
	 * getExample: sucess 1; failure 9;
	 * @throws Exception
	 */
	@Test
	public void testMovingAsteroids() throws Exception {
		  PigServer pigServer = new PigServer(pigContext);

		  pigServer.registerQuery("A = load " + Y
	              + " using PigStorage() as (rowv : double, colv : double);");
	      pigServer.registerQuery("B = filter A by  (org.apache.pig.piggybank.evaluation.math.POW(rowv,2.0) + org.apache.pig.piggybank.evaluation.math.POW(colv, 2.0)) > 50.0 and rowv >= 0.0 and colv >=0.0 ;");
	      Map<Operator, DataBag> derivedData = pigServer.getExamples("B");

	      assertTrue(derivedData != null);
	}
	
	
  	

}
