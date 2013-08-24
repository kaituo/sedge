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

public class TestMergingCondition27 {
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
		GalaxyPair3 = "'" + "/home/kaituo/code/pig3/trunk/SDSSRandomData/10/MergingCondition/GalaxyPair0" + "'";
		GalaxyPair4 = "'" + "/home/kaituo/code/pig3/trunk/SDSSRandomData/10/MergingCondition/GalaxyPair1" + "'";
		neighbors = "'" + "/home/kaituo/code/pig3/trunk/SDSSRandomData/10/MergingCondition/neighbors0" + "'";
	}
	
	@Test
	public void testFilter27() throws Exception {
		System.out.println("testFilter27");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0 and petrorad_g1 > 0.0 and petrorad_r1 > 0.0 and petrorad_i1 > 0.0 and petrorad_z1 > 0.0 and petroRadErr_g1 > 0.0 and petroMag_g1 >= 16.0 and  petroMag_g1 <= 21.0 and modelMag_u1 > -9999.0 and modelMag_g1 > -9999.0 and modelMag_r1 > -9999.0 and modelMag_i1 > -9999.0 and modelMag_z1 > -9999.0;");
		pigServer.registerQuery("G2 = load " + GalaxyPair4
				+ " using PigStorage() as (objID2 : long, ra : float, dec : float, modelMag_u2 : float, modelMag_g2 : float, modelMag_r2 : float, modelMag_i2 : float, modelMag_z2 : float, petroR50_r2 : float, petrorad_u2 : float, petrorad_g2 : float, petrorad_r2 : float, petrorad_i2 : float, petrorad_z2 : float, petroRadErr_g2: float, petroMag_g2 : float);");
		pigServer.registerQuery("AG2 = filter G2 by petrorad_u2 > 0.0 and petrorad_g2 > 0.0 and petrorad_r2 > 0.0 and petrorad_i2 > 0.0 and petrorad_z2 > 0.0 and petroRadErr_g2 > 0.0 and petroMag_g2 >= 16.0 and  petroMag_g2 <= 21.0 and modelMag_u2 > -9999.0 and modelMag_g2 > -9999.0 and modelMag_r2 > -9999.0 and modelMag_i2 > -9999.0 and modelMag_z2 > -9999.0;");
		pigServer.registerQuery("N = load " + neighbors
				+ " using PigStorage() as (objID3 : long, NeighborObjID : long, neighborType : int, distance : float);");
		pigServer.registerQuery("AN = filter N by neighborType == 3;");
		pigServer.registerQuery("D = join AN by objID3, AG1 by objID1;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("AG2");
		Map<Operator, DataBag> derivedData2 = pigServer.getExamples2("D");
		
		assertTrue(derivedData != null);
		assertTrue(derivedData2 != null);
	}//
	
	@Test
	public void testFilter28() throws Exception {
		System.out.println("testFilter28");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0 and petrorad_g1 > 0.0 and petrorad_r1 > 0.0 and petrorad_i1 > 0.0 and petrorad_z1 > 0.0 and petroRadErr_g1 > 0.0 and petroMag_g1 >= 16.0 and  petroMag_g1 <= 21.0 and modelMag_u1 > -9999.0 and modelMag_g1 > -9999.0 and modelMag_r1 > -9999.0 and modelMag_i1 > -9999.0 and modelMag_z1 > -9999.0;");
		pigServer.registerQuery("G2 = load " + GalaxyPair4
				+ " using PigStorage() as (objID2 : long, ra : float, dec : float, modelMag_u2 : float, modelMag_g2 : float, modelMag_r2 : float, modelMag_i2 : float, modelMag_z2 : float, petroR50_r2 : float, petrorad_u2 : float, petrorad_g2 : float, petrorad_r2 : float, petrorad_i2 : float, petrorad_z2 : float, petroRadErr_g2: float, petroMag_g2 : float);");
		pigServer.registerQuery("AG2 = filter G2 by petrorad_u2 > 0.0 and petrorad_g2 > 0.0 and petrorad_r2 > 0.0 and petrorad_i2 > 0.0 and petrorad_z2 > 0.0 and petroRadErr_g2 > 0.0 and petroMag_g2 >= 16.0 and  petroMag_g2 <= 21.0 and modelMag_u2 > -9999.0 and modelMag_g2 > -9999.0 and modelMag_r2 > -9999.0 and modelMag_i2 > -9999.0 and modelMag_z2 > -9999.0;");
		pigServer.registerQuery("N = load " + neighbors
				+ " using PigStorage() as (objID3 : long, NeighborObjID : long, neighborType : int, distance : float);");
		pigServer.registerQuery("AN = filter N by neighborType == 3;");
		pigServer.registerQuery("D = join AN by objID3, AG1 by objID1;");
		pigServer.registerQuery("E = join D by NeighborObjID, AG2 by objID2;");
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("E");
		
		assertTrue(derivedData != null);
	}//
	
	/**
	 * Chris's implementation fail to execute from here
	 * @throws Exception
	 */
	@Test
	public void testFilter29() throws Exception {
		System.out.println("testFilter29");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair3
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u1 : float, modelMag_g1 : float, modelMag_r1 : float, modelMag_i1 : float, modelMag_z1 : float, petroR50_r1 : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g1 : float, petroMag_g1 : float);");
		pigServer.registerQuery("AG1 = filter G1 by petrorad_u1 > 0.0 and petrorad_g1 > 0.0 and petrorad_r1 > 0.0 and petrorad_i1 > 0.0 and petrorad_z1 > 0.0 and petroRadErr_g1 > 0.0 and petroMag_g1 >= 16.0 and  petroMag_g1 <= 21.0 and modelMag_u1 > -9999.0 and modelMag_g1 > -9999.0 and modelMag_r1 > -9999.0 and modelMag_i1 > -9999.0 and modelMag_z1 > -9999.0;");
		pigServer.registerQuery("G2 = load " + GalaxyPair4
				+ " using PigStorage() as (objID2 : long, ra : float, dec : float, modelMag_u2 : float, modelMag_g2 : float, modelMag_r2 : float, modelMag_i2 : float, modelMag_z2 : float, petroR50_r2 : float, petrorad_u2 : float, petrorad_g2 : float, petrorad_r2 : float, petrorad_i2 : float, petrorad_z2 : float, petroRadErr_g2: float, petroMag_g2 : float);");
		pigServer.registerQuery("AG2 = filter G2 by petrorad_u2 > 0.0 and petrorad_g2 > 0.0 and petrorad_r2 > 0.0 and petrorad_i2 > 0.0 and petrorad_z2 > 0.0 and petroRadErr_g2 > 0.0 and petroMag_g2 >= 16.0 and  petroMag_g2 <= 21.0 and modelMag_u2 > -9999.0 and modelMag_g2 > -9999.0 and modelMag_r2 > -9999.0 and modelMag_i2 > -9999.0 and modelMag_z2 > -9999.0;");
		pigServer.registerQuery("N = load " + neighbors
				+ " using PigStorage() as (objID3 : long, NeighborObjID : long, neighborType : int, distance : float);");
		pigServer.registerQuery("AN = filter N by neighborType == 3;");
		pigServer.registerQuery("D = join AN by objID3, AG1 by objID1;");
		pigServer.registerQuery("E = join D by NeighborObjID, AG2 by objID2;");
		pigServer.registerQuery("F = filter E by objID1 < objID2;");//
		
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("F");
		
		assertTrue(derivedData != null);
	}//

}
