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

public class TestMergingConditionC35 {
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
		GalaxyPair3 = "'" + "/home/kaituo/code/pig3/trunk/SDSSRandomData/1000/MergingCondition/GalaxyPair0" + "'";
		GalaxyPair4 = "'" + "/home/kaituo/code/pig3/trunk/SDSSRandomData/1000/MergingCondition/GalaxyPair1" + "'";
		neighbors = "'" + "/home/kaituo/code/pig3/trunk/SDSSRandomData/1000/MergingCondition/neighbors0" + "'";
	}
	
	@Test
	public void testFilter35() throws Exception {
		System.out.println("testFilter35");

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
		pigServer.registerQuery("LG1 = filter KG1 by modelMag_i1 > -9999.0;");
		pigServer.registerQuery("MG1 = filter LG1 by modelMag_z1 > -9999.0;");		
		pigServer.registerQuery("G2 = load " + GalaxyPair4
				+ " using PigStorage() as (objID2 : long, ra : float, dec : float, modelMag_u2 : float, modelMag_g2 : float, modelMag_r2 : float, modelMag_i2 : float, modelMag_z2 : float, petroR50_r2 : float, petrorad_u2 : float, petrorad_g2 : float, petrorad_r2 : float, petrorad_i2 : float, petrorad_z2 : float, petroRadErr_g2: float, petroMag_g2 : float);");
		pigServer.registerQuery("AG2 = filter G2 by petrorad_u2 > 0.0;");
		pigServer.registerQuery("BG2 = filter AG2 by petrorad_g2 > 0.0;");
		pigServer.registerQuery("CG2 = filter BG2 by petrorad_r2 > 0.0;");
		pigServer.registerQuery("DG2 = filter CG2 by petrorad_i2 > 0.0;");
		pigServer.registerQuery("EG2 = filter DG2 by petrorad_z2 > 0.0;");
		pigServer.registerQuery("FG2 = filter EG2 by petroRadErr_g2 > 0.0;");
		pigServer.registerQuery("GG2 = filter FG2 by petroMag_g2 >= 16.0;");
		pigServer.registerQuery("HG2 = filter GG2 by petroMag_g2 <= 21.0;");
		pigServer.registerQuery("IG2 = filter HG2 by modelMag_u2 > -9999.0;");
		pigServer.registerQuery("JG2 = filter IG2 by modelMag_g2 > -9999.0;");
		pigServer.registerQuery("KG2 = filter JG2 by modelMag_r2 > -9999.0;");
		pigServer.registerQuery("LG2 = filter KG2 by modelMag_i2 > -9999.0;");
		pigServer.registerQuery("MG2 = filter LG2 by modelMag_z2 > -9999.0;");
		pigServer.registerQuery("N = load " + neighbors
				+ " using PigStorage() as (objID3 : long, NeighborObjID : long, neighborType : int, distance : float);");
		pigServer.registerQuery("AN = filter N by neighborType == 3;");
		pigServer.registerQuery("D = join AN by objID3, MG1 by objID1;");
		pigServer.registerQuery("E = join D by NeighborObjID, MG2 by objID2;");
		pigServer.registerQuery("F = filter E by objID1 < objID2;");//
		pigServer.registerQuery("H = filter F by modelMag_g1 - modelMag_g2 > 3.0 or modelMag_g1 - modelMag_g2 < -3.0;");
		pigServer.registerQuery("I = filter H by petroR50_r1 >= 0.25*petroR50_r2;");
		pigServer.registerQuery("J = filter I by petroR50_r1 <= 4.0*petroR50_r2;");
		pigServer.registerQuery("K = filter J by petroR50_r2 >= 0.25*petroR50_r1;");


		Map<Operator, DataBag> derivedData = pigServer.getExamples("K");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter36() throws Exception {
		System.out.println("testFilter36");

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
		pigServer.registerQuery("LG1 = filter KG1 by modelMag_i1 > -9999.0;");
		pigServer.registerQuery("MG1 = filter LG1 by modelMag_z1 > -9999.0;");		
		pigServer.registerQuery("G2 = load " + GalaxyPair4
				+ " using PigStorage() as (objID2 : long, ra : float, dec : float, modelMag_u2 : float, modelMag_g2 : float, modelMag_r2 : float, modelMag_i2 : float, modelMag_z2 : float, petroR50_r2 : float, petrorad_u2 : float, petrorad_g2 : float, petrorad_r2 : float, petrorad_i2 : float, petrorad_z2 : float, petroRadErr_g2: float, petroMag_g2 : float);");
		pigServer.registerQuery("AG2 = filter G2 by petrorad_u2 > 0.0;");
		pigServer.registerQuery("BG2 = filter AG2 by petrorad_g2 > 0.0;");
		pigServer.registerQuery("CG2 = filter BG2 by petrorad_r2 > 0.0;");
		pigServer.registerQuery("DG2 = filter CG2 by petrorad_i2 > 0.0;");
		pigServer.registerQuery("EG2 = filter DG2 by petrorad_z2 > 0.0;");
		pigServer.registerQuery("FG2 = filter EG2 by petroRadErr_g2 > 0.0;");
		pigServer.registerQuery("GG2 = filter FG2 by petroMag_g2 >= 16.0;");
		pigServer.registerQuery("HG2 = filter GG2 by petroMag_g2 <= 21.0;");
		pigServer.registerQuery("IG2 = filter HG2 by modelMag_u2 > -9999.0;");
		pigServer.registerQuery("JG2 = filter IG2 by modelMag_g2 > -9999.0;");
		pigServer.registerQuery("KG2 = filter JG2 by modelMag_r2 > -9999.0;");
		pigServer.registerQuery("LG2 = filter KG2 by modelMag_i2 > -9999.0;");
		pigServer.registerQuery("MG2 = filter LG2 by modelMag_z2 > -9999.0;");
		pigServer.registerQuery("N = load " + neighbors
				+ " using PigStorage() as (objID3 : long, NeighborObjID : long, neighborType : int, distance : float);");
		pigServer.registerQuery("AN = filter N by neighborType == 3;");
		pigServer.registerQuery("D = join AN by objID3, MG1 by objID1;");
		pigServer.registerQuery("E = join D by NeighborObjID, MG2 by objID2;");
		pigServer.registerQuery("F = filter E by objID1 < objID2;");//
		pigServer.registerQuery("H = filter F by modelMag_g1 - modelMag_g2 > 3.0 or modelMag_g1 - modelMag_g2 < -3.0;");
		pigServer.registerQuery("I = filter H by petroR50_r1 >= 0.25*petroR50_r2;");
		pigServer.registerQuery("J = filter I by petroR50_r1 <= 4.0*petroR50_r2;");
		pigServer.registerQuery("K = filter J by petroR50_r2 >= 0.25*petroR50_r1;");
		pigServer.registerQuery("L = filter K by petroR50_r2 <= 4.0*petroR50_r1;");


		Map<Operator, DataBag> derivedData = pigServer.getExamples("L");

		assertTrue(derivedData != null);
	}//

}
