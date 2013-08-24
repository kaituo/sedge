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

public class TestExGen4FilterConditionNum13 {
	static PigContext pigContext = new PigContext(ExecType.LOCAL,
			new Properties());

	static String GalaxyPair1, GalaxyPair2, neighbors, GalaxyPair3, GalaxyPair4;
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
		GalaxyPair1 = "'" + "/home/kaituo/code/pig3/trunk/SDSSRealData/GalaxyPair1" + "'";
		GalaxyPair2 = "'" + "/home/kaituo/code/pig3/trunk/SDSSRealData/GalaxyPair2" + "'";
		GalaxyPair3 = "'" + "/home/kaituo/code/pig3/trunk/SDSSRealData/GalaxyPair3" + "'";
		GalaxyPair4 = "'" + "/home/kaituo/code/pig3/trunk/SDSSRealData/GalaxyPair4" + "'";
		neighbors = "'" + "/home/kaituo/code/pig3/trunk/SDSSRealData/neighbors" + "'";
		
	}
	
	@Test
	public void testFilter13() throws Exception {

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair1
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u : float, modelMag_g : float, modelMag_r : float, modelMag_i : float, modelMag_z : float, petroR50_r : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float);");
		pigServer.registerQuery("G2 = load " + GalaxyPair2
				+ " using PigStorage() as (objID2 : long, ra : float, dec : float, modelMag_u : float, modelMag_g : float, modelMag_r : float, modelMag_i : float, modelMag_z : float, petroR50_r : float, petrorad_u2 : float, petrorad_g2 : float, petrorad_r2 : float, petrorad_i2 : float, petrorad_z2 : float, petroRadErr_g2: float);");
		pigServer.registerQuery("N = load " + neighbors
				+ " using PigStorage() as (objID3 : long, NeighborObjID : long, neighborType : int, distance : float);");
		pigServer.registerQuery("D = join N by objID3, G1 by objID1;");
		pigServer.registerQuery("E = join D by NeighborObjID, G2 by objID2;");
		pigServer.registerQuery("F = filter E by petrorad_u1 > 0.0 and petrorad_u2 > 0.0 and petrorad_g1 > 0.0 and petrorad_g2 > 0.0 and objID1 < objID2 and neighborType == 3 and petrorad_r1 > 0.0 and petrorad_r2 > 0.0 and petrorad_i1 > 0.0 and petrorad_i2 > 0.0 and petrorad_z1 > 0.0 and petrorad_z2 > 0.0 and petroRadErr_g1 > 0.0;");//

		Map<Operator, DataBag> derivedData = pigServer.getExamples2("F");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter14() throws Exception {

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u : float, modelMag_g : float, modelMag_r : float, modelMag_i : float, modelMag_z : float, petroR50_r : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("G2 = load " + GalaxyPair4
				+ " using PigStorage() as (objID2 : long, ra : float, dec : float, modelMag_u : float, modelMag_g : float, modelMag_r : float, modelMag_i : float, modelMag_z : float, petroR50_r : float, petrorad_u2 : float, petrorad_g2 : float, petrorad_r2 : float, petrorad_i2 : float, petrorad_z2 : float, petroRadErr_g2: float, petroMag_g2 : float);");
		pigServer.registerQuery("N = load " + neighbors
				+ " using PigStorage() as (objID3 : long, NeighborObjID : long, neighborType : int, distance : float);");
		pigServer.registerQuery("D = join N by objID3, G1 by objID1;");
		pigServer.registerQuery("E = join D by NeighborObjID, G2 by objID2;");
		pigServer.registerQuery("F = filter E by petrorad_u1 > 0.0 and petrorad_u2 > 0.0 and petrorad_g1 > 0.0 and petrorad_g2 > 0.0 and objID1 < objID2 and neighborType == 3 and petrorad_r1 > 0.0 and petrorad_r2 > 0.0 and petrorad_i1 > 0.0 and petrorad_i2 > 0.0 and petrorad_z1 > 0.0 and petrorad_z2 > 0.0 and petroRadErr_g1 > 0.0 and petroRadErr_g2 > 0.0 and petroMag_g1 >= 16.0 and petroMag_g1 <= 21.0;");//

		Map<Operator, DataBag> derivedData = pigServer.getExamples2("F");

		assertTrue(derivedData != null);
	}//
	
	

}
