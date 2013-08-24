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

public class TestExGen4FilterConditionNum7 {
	static PigContext pigContext = new PigContext(ExecType.LOCAL,
			new Properties());

	static String GalaxyPair1, GalaxyPair2, neighbors;
//	static File filePhotoObj;

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
//		filePhotoObj = File.createTempFile("dataPhotoObj", ".dat");
//		writeData(filePhotoObj);
//		filePhotoObj.deleteOnExit();
		/**
		 * select top 10 objid from Galaxy
		 */
		GalaxyPair1 = "'" + "/home/kaituo/code/pig3/trunk/SDSSRealData/GalaxyPair1" + "'";
		GalaxyPair2 = "'" + "/home/kaituo/code/pig3/trunk/SDSSRealData/GalaxyPair2" + "'";
		neighbors = "'" + "/home/kaituo/code/pig3/trunk/SDSSRealData/neighbors" + "'";
		
	}
	
	@Test
	public void testFilter7() throws Exception {

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair1
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u : float, modelMag_g : float, modelMag_r : float, modelMag_i : float, modelMag_z : float, petroR50_r : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z : float, petroRadErr_g : float);");
		pigServer.registerQuery("G2 = load " + GalaxyPair2
				+ " using PigStorage() as (objID2 : long, ra : float, dec : float, modelMag_u : float, modelMag_g : float, modelMag_r : float, modelMag_i : float, modelMag_z : float, petroR50_r : float, petrorad_u2 : float, petrorad_g2 : float, petrorad_r2 : float, petrorad_i2 : float, petrorad_z : float, petroRadErr_g : float);");
		pigServer.registerQuery("N = load " + neighbors
				+ " using PigStorage() as (objID3 : long, NeighborObjID : long, neighborType : int, distance : float);");
		pigServer.registerQuery("D = join N by objID3, G1 by objID1;");
		pigServer.registerQuery("E = join D by NeighborObjID, G2 by objID2;");
		pigServer.registerQuery("F = filter E by petrorad_u1 > 0.0 and petrorad_u2 > 0.0 and petrorad_g1 > 0.0 and petrorad_g2 > 0.0 and objID1 < objID2 and neighborType == 3 and petrorad_r1 > 0.0;");//

		Map<Operator, DataBag> derivedData = pigServer.getExamples2("F");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter8() throws Exception {

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair1
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u : float, modelMag_g : float, modelMag_r : float, modelMag_i : float, modelMag_z : float, petroR50_r : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z : float, petroRadErr_g : float);");
		pigServer.registerQuery("G2 = load " + GalaxyPair2
				+ " using PigStorage() as (objID2 : long, ra : float, dec : float, modelMag_u : float, modelMag_g : float, modelMag_r : float, modelMag_i : float, modelMag_z : float, petroR50_r : float, petrorad_u2 : float, petrorad_g2 : float, petrorad_r2 : float, petrorad_i2 : float, petrorad_z : float, petroRadErr_g : float);");
		pigServer.registerQuery("N = load " + neighbors
				+ " using PigStorage() as (objID3 : long, NeighborObjID : long, neighborType : int, distance : float);");
		pigServer.registerQuery("D = join N by objID3, G1 by objID1;");
		pigServer.registerQuery("E = join D by NeighborObjID, G2 by objID2;");
		pigServer.registerQuery("F = filter E by petrorad_u1 > 0.0 and petrorad_u2 > 0.0 and petrorad_g1 > 0.0 and petrorad_g2 > 0.0 and objID1 < objID2 and neighborType == 3 and petrorad_r1 > 0.0 and petrorad_r2 > 0.0;");//

		Map<Operator, DataBag> derivedData = pigServer.getExamples2("F");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter9() throws Exception {

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair1
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u : float, modelMag_g : float, modelMag_r : float, modelMag_i : float, modelMag_z : float, petroR50_r : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z : float, petroRadErr_g : float);");
		pigServer.registerQuery("G2 = load " + GalaxyPair2
				+ " using PigStorage() as (objID2 : long, ra : float, dec : float, modelMag_u : float, modelMag_g : float, modelMag_r : float, modelMag_i : float, modelMag_z : float, petroR50_r : float, petrorad_u2 : float, petrorad_g2 : float, petrorad_r2 : float, petrorad_i2 : float, petrorad_z : float, petroRadErr_g : float);");
		pigServer.registerQuery("N = load " + neighbors
				+ " using PigStorage() as (objID3 : long, NeighborObjID : long, neighborType : int, distance : float);");
		pigServer.registerQuery("D = join N by objID3, G1 by objID1;");
		pigServer.registerQuery("E = join D by NeighborObjID, G2 by objID2;");
		pigServer.registerQuery("F = filter E by petrorad_u1 > 0.0 and petrorad_u2 > 0.0 and petrorad_g1 > 0.0 and petrorad_g2 > 0.0 and objID1 < objID2 and neighborType == 3 and petrorad_r1 > 0.0 and petrorad_r2 > 0.0 and petrorad_i1 > 0.0;");//

		Map<Operator, DataBag> derivedData = pigServer.getExamples2("F");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter10() throws Exception {

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair1
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u : float, modelMag_g : float, modelMag_r : float, modelMag_i : float, modelMag_z : float, petroR50_r : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z : float, petroRadErr_g : float);");
		pigServer.registerQuery("G2 = load " + GalaxyPair2
				+ " using PigStorage() as (objID2 : long, ra : float, dec : float, modelMag_u : float, modelMag_g : float, modelMag_r : float, modelMag_i : float, modelMag_z : float, petroR50_r : float, petrorad_u2 : float, petrorad_g2 : float, petrorad_r2 : float, petrorad_i2 : float, petrorad_z : float, petroRadErr_g : float);");
		pigServer.registerQuery("N = load " + neighbors
				+ " using PigStorage() as (objID3 : long, NeighborObjID : long, neighborType : int, distance : float);");
		pigServer.registerQuery("D = join N by objID3, G1 by objID1;");
		pigServer.registerQuery("E = join D by NeighborObjID, G2 by objID2;");
		pigServer.registerQuery("F = filter E by petrorad_u1 > 0.0 and petrorad_u2 > 0.0 and petrorad_g1 > 0.0 and petrorad_g2 > 0.0 and objID1 < objID2 and neighborType == 3 and petrorad_r1 > 0.0 and petrorad_r2 > 0.0 and petrorad_i1 > 0.0 and petrorad_i2 > 0.0;");//

		Map<Operator, DataBag> derivedData = pigServer.getExamples2("F");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter11() throws Exception {

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair1
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u : float, modelMag_g : float, modelMag_r : float, modelMag_i : float, modelMag_z : float, petroR50_r : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g : float);");
		pigServer.registerQuery("G2 = load " + GalaxyPair2
				+ " using PigStorage() as (objID2 : long, ra : float, dec : float, modelMag_u : float, modelMag_g : float, modelMag_r : float, modelMag_i : float, modelMag_z : float, petroR50_r : float, petrorad_u2 : float, petrorad_g2 : float, petrorad_r2 : float, petrorad_i2 : float, petrorad_z2 : float, petroRadErr_g : float);");
		pigServer.registerQuery("N = load " + neighbors
				+ " using PigStorage() as (objID3 : long, NeighborObjID : long, neighborType : int, distance : float);");
		pigServer.registerQuery("D = join N by objID3, G1 by objID1;");
		pigServer.registerQuery("E = join D by NeighborObjID, G2 by objID2;");
		pigServer.registerQuery("F = filter E by petrorad_u1 > 0.0 and petrorad_u2 > 0.0 and petrorad_g1 > 0.0 and petrorad_g2 > 0.0 and objID1 < objID2 and neighborType == 3 and petrorad_r1 > 0.0 and petrorad_r2 > 0.0 and petrorad_i1 > 0.0 and petrorad_i2 > 0.0 and petrorad_z1 > 0.0;");//

		Map<Operator, DataBag> derivedData = pigServer.getExamples2("F");

		assertTrue(derivedData != null);
	}//
	
	@Test
	public void testFilter12() throws Exception {

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("G1 = load " + GalaxyPair1
				+ " using PigStorage() as (objID1 : long, ra : float, dec : float, modelMag_u : float, modelMag_g : float, modelMag_r : float, modelMag_i : float, modelMag_z : float, petroR50_r : float, petrorad_u1 : float, petrorad_g1 : float, petrorad_r1 : float, petrorad_i1 : float, petrorad_z1 : float, petroRadErr_g : float);");
		pigServer.registerQuery("G2 = load " + GalaxyPair2
				+ " using PigStorage() as (objID2 : long, ra : float, dec : float, modelMag_u : float, modelMag_g : float, modelMag_r : float, modelMag_i : float, modelMag_z : float, petroR50_r : float, petrorad_u2 : float, petrorad_g2 : float, petrorad_r2 : float, petrorad_i2 : float, petrorad_z2 : float, petroRadErr_g : float);");
		pigServer.registerQuery("N = load " + neighbors
				+ " using PigStorage() as (objID3 : long, NeighborObjID : long, neighborType : int, distance : float);");
		pigServer.registerQuery("D = join N by objID3, G1 by objID1;");
		pigServer.registerQuery("E = join D by NeighborObjID, G2 by objID2;");
		pigServer.registerQuery("F = filter E by petrorad_u1 > 0.0 and petrorad_u2 > 0.0 and petrorad_g1 > 0.0 and petrorad_g2 > 0.0 and objID1 < objID2 and neighborType == 3 and petrorad_r1 > 0.0 and petrorad_r2 > 0.0 and petrorad_i1 > 0.0 and petrorad_i2 > 0.0 and petrorad_z1 > 0.0 and petrorad_z2 > 0.0;");//

		Map<Operator, DataBag> derivedData = pigServer.getExamples2("F");

		assertTrue(derivedData != null);
	}//
}
