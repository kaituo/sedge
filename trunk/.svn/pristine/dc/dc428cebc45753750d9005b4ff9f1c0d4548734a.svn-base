package edu.umass.cs.pig.test.merging;

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
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0;");
		pigServer.registerQuery("BG1 = filter AG1 by petrorad_g1 > 0.0;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("BG1");

		assertTrue(derivedData != null);
	}//

	@Test
	public void testFilter3() throws Exception {
		System.out.println("testFilter3");
		
		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0;");
		pigServer.registerQuery("BG1 = filter AG1 by petrorad_g1 > 0.0;");
		pigServer.registerQuery("CG1 = filter BG1 by petrorad_r1 > 0.0;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("CG1");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter4() throws Exception {
		System.out.println("testFilter4");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0;");
		pigServer.registerQuery("BG1 = filter AG1 by petrorad_g1 > 0.0;");
		pigServer.registerQuery("CG1 = filter BG1 by petrorad_r1 > 0.0;");
		pigServer.registerQuery("DG1 = filter CG1 by petrorad_i1 > 0.0;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("DG1");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter5() throws Exception {
		System.out.println("testFilter5");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0;");
		pigServer.registerQuery("BG1 = filter AG1 by petrorad_g1 > 0.0;");
		pigServer.registerQuery("CG1 = filter BG1 by petrorad_r1 > 0.0;");
		pigServer.registerQuery("DG1 = filter CG1 by petrorad_i1 > 0.0;");
		pigServer.registerQuery("EG1 = filter DG1 by petrorad_z1 > 0.0;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("EG1");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter6() throws Exception {
		System.out.println("testFilter6");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0;");
		pigServer.registerQuery("BG1 = filter AG1 by petrorad_g1 > 0.0;");
		pigServer.registerQuery("CG1 = filter BG1 by petrorad_r1 > 0.0;");
		pigServer.registerQuery("DG1 = filter CG1 by petrorad_i1 > 0.0;");
		pigServer.registerQuery("EG1 = filter DG1 by petrorad_z1 > 0.0;");
		pigServer.registerQuery("FG1 = filter EG1 by petroRadErr_g1 > 0.0;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("FG1");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter7() throws Exception {
		System.out.println("testFilter7");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0;");
		pigServer.registerQuery("BG1 = filter AG1 by petrorad_g1 > 0.0;");
		pigServer.registerQuery("CG1 = filter BG1 by petrorad_r1 > 0.0;");
		pigServer.registerQuery("DG1 = filter CG1 by petrorad_i1 > 0.0;");
		pigServer.registerQuery("EG1 = filter DG1 by petrorad_z1 > 0.0;");
		pigServer.registerQuery("FG1 = filter EG1 by petroRadErr_g1 > 0.0;");
		pigServer.registerQuery("GG1 = filter FG1 by petroMag_g1 >= 16.0;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("GG1");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter8() throws Exception {
		System.out.println("testFilter8");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0;");
		pigServer.registerQuery("BG1 = filter AG1 by petrorad_g1 > 0.0;");
		pigServer.registerQuery("CG1 = filter BG1 by petrorad_r1 > 0.0;");
		pigServer.registerQuery("DG1 = filter CG1 by petrorad_i1 > 0.0;");
		pigServer.registerQuery("EG1 = filter DG1 by petrorad_z1 > 0.0;");
		pigServer.registerQuery("FG1 = filter EG1 by petroRadErr_g1 > 0.0;");
		pigServer.registerQuery("GG1 = filter FG1 by petroMag_g1 >= 16.0;");
		pigServer.registerQuery("HG1 = filter GG1 by petroMag_g1 <= 21.0;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("HG1");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter9() throws Exception {
		System.out.println("testFilter9");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0;");
		pigServer.registerQuery("BG1 = filter AG1 by petrorad_g1 > 0.0;");
		pigServer.registerQuery("CG1 = filter BG1 by petrorad_r1 > 0.0;");
		pigServer.registerQuery("DG1 = filter CG1 by petrorad_i1 > 0.0;");
		pigServer.registerQuery("EG1 = filter DG1 by petrorad_z1 > 0.0;");
		pigServer.registerQuery("FG1 = filter EG1 by petroRadErr_g1 > 0.0;");
		pigServer.registerQuery("GG1 = filter FG1 by petroMag_g1 >= 16.0;");
		pigServer.registerQuery("HG1 = filter GG1 by petroMag_g1 <= 21.0;");
		pigServer.registerQuery("IG1 = filter HG1 by modelMag_u1 > -9999.0;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("IG1");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter10() throws Exception {
		System.out.println("testFilter10");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0;");
		pigServer.registerQuery("BG1 = filter AG1 by petrorad_g1 > 0.0;");
		pigServer.registerQuery("CG1 = filter BG1 by petrorad_r1 > 0.0;");
		pigServer.registerQuery("DG1 = filter CG1 by petrorad_i1 > 0.0;");
		pigServer.registerQuery("EG1 = filter DG1 by petrorad_z1 > 0.0;");
		pigServer.registerQuery("FG1 = filter EG1 by petroRadErr_g1 > 0.0;");
		pigServer.registerQuery("GG1 = filter FG1 by petroMag_g1 >= 16.0;");
		pigServer.registerQuery("HG1 = filter GG1 by petroMag_g1 <= 21.0;");
		pigServer.registerQuery("IG1 = filter HG1 by modelMag_u1 > -9999.0;");
		pigServer.registerQuery("JG1 = filter IG1 by modelMag_g1 > -9999.0;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("JG1");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter11() throws Exception {
		System.out.println("testFilter11");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0;");
		pigServer.registerQuery("BG1 = filter AG1 by petrorad_g1 > 0.0;");
		pigServer.registerQuery("CG1 = filter BG1 by petrorad_r1 > 0.0;");
		pigServer.registerQuery("DG1 = filter CG1 by petrorad_i1 > 0.0;");
		pigServer.registerQuery("EG1 = filter DG1 by petrorad_z1 > 0.0;");
		pigServer.registerQuery("FG1 = filter EG1 by petroRadErr_g1 > 0.0;");
		pigServer.registerQuery("GG1 = filter FG1 by petroMag_g1 >= 16.0;");
		pigServer.registerQuery("HG1 = filter GG1 by petroMag_g1 <= 21.0;");
		pigServer.registerQuery("IG1 = filter HG1 by modelMag_u1 > -9999.0;");
		pigServer.registerQuery("JG1 = filter IG1 by modelMag_g1 > -9999.0;");
		pigServer.registerQuery("KG1 = filter JG1 by modelMag_r1 > -9999.0;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("KG1");

		assertTrue(derivedData != null);
	}//
}
