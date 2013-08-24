package edu.umass.cs.pig.test;

import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.Operator;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMergingCondition1 {
	static PigContext pigContext = new PigContext(ExecType.LOCAL,
			new Properties());

	static String neighbors, GalaxyPair3, GalaxyPair4;
//	static File filePhotoObj;

	{
		try {
			pigContext.connect();
		} catch (ExecException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * For GalaxyPair3, GalaxyPair4: SELECT top 20 objID, ra, dec, modelMag_u,
	 * modelMag_g, modelMag_r, modelMag_i, modelMag_z, petroR50_r, petrorad_u,
	 * petrorad_g, petrorad_r, petrorad_i, petrorad_z, petroRadErr_g, petroMag_g
	 * from Galaxy
	 */
	@BeforeClass
	public static void oneTimeSetup() throws Exception {
//		filePhotoObj = File.createTempFile("dataPhotoObj", ".dat");
//		writeData(filePhotoObj);
//		filePhotoObj.deleteOnExit();
		/**
		 * select top 10 objid from Galaxy
		 */
//		GalaxyPair1 = "'" + "/home/kaituo/code/pig3/trunk/SDSSRealData/GalaxyPair1" + "'";
//		GalaxyPair2 = "'" + "/home/kaituo/code/pig3/trunk/SDSSRealData/GalaxyPair2" + "'";
		GalaxyPair3 = "'" + "/home/kaituo/code/pig3/trunk/SDSSRandomData/10/MergingCondition/GalaxyPair0" + "'";
		GalaxyPair4 = "'" + "/home/kaituo/code/pig3/trunk/SDSSRandomData/10/MergingCondition/GalaxyPair1" + "'";
		neighbors = "'" + "/home/kaituo/code/pig3/trunk/SDSSRandomData/10/MergingCondition/neighbors0" + "'";
		
	}
	
	@Test
	public void testFilter1() throws Exception {
		System.out.println("testFilter1");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("AG1");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter2() throws Exception {
		System.out.println("testFilter2");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0 and petrorad_g1 > 0.0;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("AG1");

		assertTrue(derivedData != null);
	}//

	@Test
	public void testFilter3() throws Exception {
		System.out.println("testFilter3");
		
		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0 and petrorad_g1 > 0.0 and petrorad_r1 > 0.0;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("AG1");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter4() throws Exception {
		System.out.println("testFilter4");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0 and petrorad_g1 > 0.0 and petrorad_r1 > 0.0 and petrorad_i1 > 0.0;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("AG1");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter5() throws Exception {
		System.out.println("testFilter5");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0 and petrorad_g1 > 0.0 and petrorad_r1 > 0.0 and petrorad_i1 > 0.0 and  petrorad_z1 > 0.0;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("AG1");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter6() throws Exception {
		System.out.println("testFilter6");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0 and petrorad_g1 > 0.0 and petrorad_r1 > 0.0 and petrorad_i1 > 0.0 and  petrorad_z1 > 0.0 and petroRadErr_g1 > 0.0;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("AG1");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter7() throws Exception {
		System.out.println("testFilter7");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0 and petrorad_g1 > 0.0 and petrorad_r1 > 0.0 and petrorad_i1 > 0.0 and  petrorad_z1 > 0.0 and petroRadErr_g1 > 0.0 and petroMag_g1 >= 16.0;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("AG1");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter8() throws Exception {
		System.out.println("testFilter8");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0 and petrorad_g1 > 0.0 and petrorad_r1 > 0.0 and petrorad_i1 > 0.0 and  petrorad_z1 > 0.0 and petroRadErr_g1 > 0.0 and petroMag_g1 >= 16.0 and  petroMag_g1 <= 21.0;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("AG1");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter9() throws Exception {
		System.out.println("testFilter9");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0 and petrorad_g1 > 0.0 and petrorad_r1 > 0.0 and petrorad_i1 > 0.0 and  petrorad_z1 > 0.0 and petroRadErr_g1 > 0.0 and petroMag_g1 >= 16.0 and  petroMag_g1 <= 21.0 and modelMag_u1 > -9999.0;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("AG1");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter10() throws Exception {
		System.out.println("testFilter10");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0 and petrorad_g1 > 0.0 and petrorad_r1 > 0.0 and petrorad_i1 > 0.0 and  petrorad_z1 > 0.0 and petroRadErr_g1 > 0.0 and petroMag_g1 >= 16.0 and  petroMag_g1 <= 21.0 and modelMag_u1 > -9999.0 and modelMag_g1 > -9999.0;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("AG1");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter11() throws Exception {
		System.out.println("testFilter11");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0 and petrorad_g1 > 0.0 and petrorad_r1 > 0.0 and petrorad_i1 > 0.0 and  petrorad_z1 > 0.0 and petroRadErr_g1 > 0.0 and petroMag_g1 >= 16.0 and  petroMag_g1 <= 21.0 and modelMag_u1 > -9999.0 and modelMag_g1 > -9999.0 and modelMag_r1 > -9999.0;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("AG1");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter12() throws Exception {
		System.out.println("testFilter12");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0 and petrorad_g1 > 0.0 and petrorad_r1 > 0.0 and petrorad_i1 > 0.0 and  petrorad_z1 > 0.0 and petroRadErr_g1 > 0.0 and petroMag_g1 >= 16.0 and  petroMag_g1 <= 21.0 and modelMag_u1 > -9999.0 and modelMag_g1 > -9999.0 and modelMag_r1 > -9999.0 and modelMag_i1 > -9999.0;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("AG1");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter13() throws Exception {
		System.out.println("testFilter13");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0 and petrorad_g1 > 0.0 and petrorad_r1 > 0.0 and petrorad_i1 > 0.0 and  petrorad_z1 > 0.0 and petroRadErr_g1 > 0.0 and petroMag_g1 >= 16.0 and  petroMag_g1 <= 21.0 and modelMag_u1 > -9999.0 and modelMag_g1 > -9999.0 and modelMag_r1 > -9999.0 and modelMag_i1 > -9999.0 and modelMag_z1 > -9999.0;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("AG1");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter14() throws Exception {
		System.out.println("testFilter14");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0 and petrorad_g1 > 0.0 and petrorad_r1 > 0.0 and petrorad_i1 > 0.0 and  petrorad_z1 > 0.0 and petroRadErr_g1 > 0.0 and petroMag_g1 >= 16.0 and  petroMag_g1 <= 21.0 and modelMag_u1 > -9999.0 and modelMag_g1 > -9999.0 and modelMag_r1 > -9999.0 and modelMag_i1 > -9999.0 and modelMag_z1 > -9999.0;");
		pigServer.registerQuery("G2 = load " + GalaxyPair4
				+ " using PigStorage() as (objID2 : long, ra : float, dec : float, modelMag_u2 : float, modelMag_g2 : float, modelMag_r2 : float, modelMag_i2 : float, modelMag_z2 : float, petroR50_r2 : float, petrorad_u2 : float, petrorad_g2 : float, petrorad_r2 : float, petrorad_i2 : float, petrorad_z2 : float, petroRadErr_g2: float, petroMag_g2 : float);");
		pigServer.registerQuery("AG2 = filter G2 by petrorad_u2 > 0.0;");
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("AG1");
		Map<Operator, DataBag> derivedData2 = pigServer.getExamples2("AG2");

		assertTrue(derivedData != null);
		assertTrue(derivedData2 != null);
	}//
	
	@Test
	public void testFilter15() throws Exception {
		System.out.println("testFilter15");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0 and petrorad_g1 > 0.0 and petrorad_r1 > 0.0 and petrorad_i1 > 0.0 and  petrorad_z1 > 0.0 and petroRadErr_g1 > 0.0 and petroMag_g1 >= 16.0 and  petroMag_g1 <= 21.0 and modelMag_u1 > -9999.0 and modelMag_g1 > -9999.0 and modelMag_r1 > -9999.0 and modelMag_i1 > -9999.0 and modelMag_z1 > -9999.0;");
		pigServer.registerQuery("G2 = load " + GalaxyPair4
				+ " using PigStorage() as (objID2 : long, ra : float, dec : float, modelMag_u2 : float, modelMag_g2 : float, modelMag_r2 : float, modelMag_i2 : float, modelMag_z2 : float, petroR50_r2 : float, petrorad_u2 : float, petrorad_g2 : float, petrorad_r2 : float, petrorad_i2 : float, petrorad_z2 : float, petroRadErr_g2: float, petroMag_g2 : float);");
		pigServer.registerQuery("AG2 = filter G2 by petrorad_u2 > 0.0 and petrorad_g2 > 0.0;");
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("AG1");
		Map<Operator, DataBag> derivedData2 = pigServer.getExamples2("AG2");

		assertTrue(derivedData != null);
		assertTrue(derivedData2 != null);
	}//
	
	
}
