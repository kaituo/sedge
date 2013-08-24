package edu.umass.cs.pig.test.runtime.sdss;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
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

public class TestExGen4SDSS929N1_6 {
	static PigContext pigContext = new PigContext(ExecType.LOCAL,
			new Properties());

	static String PhotoObj, Galaxy, SpecClass, SpecObj, Galaxy2, Galaxy3, PhotoPrimary, Galaxy4, PhotoPrimary2, Galaxy5, PhotoObj2;//, PhotoObj3;
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
//		SpecClass = "'" + "/home/kaituo/code/pig3/trunk/SDSSRandomData/10/SpecClassFdr/specClass0.dat" + "'";
//
//		PhotoObj = "'" + "/home/kaituo/code/pig3/trunk/SDSSRandomData/100/BasicFdr/basic9.dat" + "'";
//		Galaxy = "'" + "/home/kaituo/code/pig3/trunk/SDSSRandomData/100/MergingCondition/GalaxyPair9"  + "'";
//		SpecObj = "'" + "/home/kaituo/code/pig3/trunk/SDSSRandomData/100/specObjIDFdr/specObj9.dat" + "'";
//		Galaxy2 = "'" + "/home/kaituo/code/pig3/trunk/SDSSRandomData/100/multipleCriteria/galaxy2N9.dat" + "'";
//		Galaxy3 = "'" + "/home/kaituo/code/pig3/trunk/SDSSRandomData/100/spatialUnitVectors/galaxy3N9.dat" + "'";
		PhotoPrimary = "'" + "/home/kaituo/code/pig3/trunk/SDSSRandomData/100/CVs/PhotoPrimary7.dat" + "'";
//		Galaxy4 = "'" + "/home/kaituo/code/pig3/trunk/SDSSRandomData/100/lowzQSOs/Galaxy4N9.dat" + "'";
//		PhotoPrimary2 = "'" + "/home/kaituo/code/pig3/trunk/SDSSRandomData/100/velocitiesErrors/photoPrimary2N9.dat" + "'";
//		Galaxy5 = "'" + "/home/kaituo/code/pig3/trunk/SDSSRandomData/100/bETWEEN/Galaxy5N9.dat" + "'";
//		PhotoObj2 = "'" + "/home/kaituo/code/pig3/trunk/SDSSRandomData/100/MovingAsteroids/PhotoObj2N9.dat" + "'";
//		PhotoObj3 = "'" + "/home/kaituo/code/pig3/trunk/SDSSRealData/movingAsteroids2" + "'";
		
//		System.out.println("PhotoObj : " + PhotoObj + "\n");
//		System.out.println("Test data created.");
	}
	
	/**
	 * SELECT  top 10 objID, field, ra, dec, run from PhotoObj
	 * 
	 * objID	bigint
	 * field	smallint	
	 * ra	float
	 * dec	float
	 * run	smallint
	 * 
	 * objID	       field	    ra	                 dec	     run
	758882625380943288	12	50.7087978370105	76.9631177132159	6074
	758882625380942911	12	50.7990706615098	77.0751444481533	6074
	758882625380942429	12	50.7447936093687	77.0303566953601	6074
	758882625380942275	12	50.7502262046164	76.9432157376269	6074
	758882625380942005	12	50.6069052556651	77.0319862609625	6074
	758882625380941861	12	50.5479970090686	76.9939765386199	6074
	758882625380942008	12	50.6200418736017	77.0308681539771	6074
	758882625380942031	12	50.7019597858277	77.0215462421214	6074
	758882625380942091	12	50.7423458829802	77.0451894855714	6074
	758882625380942193	12	50.8090358164206	77.0809052417237	6074
	 */
	private static void writeData(File dataFile) throws Exception {
		FileOutputStream dat = new FileOutputStream(dataFile);

		dat.write(( 758882625380943288L + "\t" + 12 + "\t" + 50.7087978370105 + "\t" + 76.9631177132159 + "\t" + 6074 + "\n")
					.getBytes());
		dat.write(( 758882625380942911L + "\t" + 12 + "\t" + 50.7990706615098 + "\t" + 77.0751444481533 + "\t" + 6074 + "\n")
				.getBytes());
		dat.write(( 758882625380942429L + "\t" + 12 + "\t" + 50.7447936093687 + "\t" + 77.0303566953601 + "\t" + 6074 + "\n")
				.getBytes());
		dat.write(( 758882625380942275L + "\t" + 12 + "\t" +  50.7502262046164 + "\t" + 76.9432157376269 + "\t" + 6074 + "\n")
				.getBytes());
		dat.write(( 758882625380942005L + "\t" + 12 + "\t" + 50.6069052556651 + "\t" + 77.0319862609625 + "\t" + 6074 + "\n")
				.getBytes());
		dat.write(( 758882625380941861L + "\t" + 12 + "\t" + 50.5479970090686 + "\t" + 76.9939765386199 + "\t" + 6074 + "\n")
				.getBytes());
		dat.write(( 758882625380942008L + "\t" +  12 + "\t" + 50.6200418736017 + "\t" + 77.0308681539771 + "\t" + 6074 + "\n")
				.getBytes());
		dat.write(( 758882625380942031L + "\t" +  12 + "\t" + 50.7019597858277 + "\t" + 77.0215462421214 + "\t" + 6074 + "\n")
				.getBytes());
		dat.write(( 758882625380942091L + "\t" + 12 + "\t" + 50.7423458829802 + "\t" + 77.0451894855714 + "\t" + 6074 + "\n")
				.getBytes());
		dat.write(( 758882625380942193L + "\t" + 12 + "\t" + 50.8090358164206 + "\t" + 77.0809052417237 + "\t" + 6074 + "\n")
				.getBytes());

		dat.close();
	}
	
	/**
	 * Basic SELECT-FROM-WHERE      Back to TopTop
	-- Returns 5261 objects in DR2 (5278 in DR1) in a few sec.
	
	-- Find objects in a particular field.
	-- A basic SELECT - FROM - WHERE query.
	
	* SELECT objID, 	-- Get the unique object ID,
	    field, ra, dec 	-- the field number, and coordinates
	FROM PhotoObj		-- From the photometric data
	WHERE run=1336 and field = 11 	-- that matches our criteria 
	
	* SELECT  top 10 objID, field, ra, dec, run from PhotoObj
	 
	 * objID	bigint
	 * field	smallint	
	 * ra	float
	 * dec	float
	 * run	smallint
	 * 
	 * @throws Exception
	 */
//	@Test
//	public void testBasic() throws Exception {
//		System.out.println("testBasic");
//		PigServer pigServer = new PigServer(pigContext);
//
//		pigServer.registerQuery("A = load " + PhotoObj
//				+ " using PigStorage() as (objID : long, field : int, ra : float, dec : float, run : int);");
//		pigServer.registerQuery("B = filter A by run == 1336 and field ==11 ;");
//		Map<Operator, DataBag> derivedData = pigServer.getExamples("B");
//
//		assertTrue(derivedData != null);
//	}//
//	
//	/**
//	 * -- Returns 1000 objects in a few sec.
//
//		-- Find all galaxies brighter than r magnitude 22, where the local
//		-- extinction is > 0.175. This is a simple query that uses a WHERE clause,
//		-- but now two conditions that must be met simultaneously. However, this
//		-- query returns a lot of galaxies (29 Million in DR2!), so it will take a
//		-- long time to get the results back. The sample therefore includes a
//		-- "TOP 1000" restriction to make it run quickly.
//		-- This query also introduces the Galaxy view, which contains the
//		-- photometric parameters (no redshifts or spectra) for unresolved objects.
//		
//	* SELECT TOP 1000 objID
//		FROM Galaxy
//		WHERE
//		    r < 22 	-- r IS NOT deredenned
//		    and extinction_r > 0.175 	-- extinction more than 0.175 
//		    
//		    
//	* SELECT top 10 objID, r, extinction_r
//		FROM Galaxy 
//		    
//	  *  objID	bigint
//	    extinction_r	real
//	    r	real
//		    
//		    
//	 * @throws Exception
//	 */
//	@Test
//	public void testGalaxies2Criteria() throws Exception {
//		System.out.println("testGalaxies2Criteria");
//		PigServer pigServer = new PigServer(pigContext);
//
//		pigServer
//				.registerQuery("A = load "
//						+ Galaxy
//						+ " using PigStorage() as (objID : long, extinction_r : double, r : double);");
//		pigServer
//				.registerQuery("B = filter A by r < 22.0 and  extinction_r > 0.175 ;");
//		Map<Operator, DataBag> derivedData = pigServer.getExamples("B");
//
//		assertTrue(derivedData != null);
//	}//
//	
//	/**
//	 * -- Find all objects with unclassified spectra.
//		-- A simple SELECT-FROM-WHERE query, using a function
//		
//		SELECT specObjID
//		FROM SpecObj
//		WHERE SpecClass = dbo.fSpecClass('UNKNOWN') 
//		
//	  * SELECT top 10 specObjID, SpecClass
//		FROM   SpecObj
//	  * unclassifiedSpectra1.dat
//	  * specObjID	bigint
//	  * specClass	smallint
//	  * 
//	  * SELECT top 10 name, value
//		FROM   SpecClass
//	  * unclassifiedSpectra2.dat
//	  * name	varchar
//	  * value	int
//		
//	 * @throws Exception
//	 */
//	@Test
//	public void testUnclassifiedSpectra() throws Exception {
//		System.out.println("testUnclassifiedSpectra");
//
//		PigServer pigServer = new PigServer(pigContext);
//
//		pigServer
//				.registerQuery("A = load "
//						+ SpecObj
//						+ " using PigStorage() as (name : chararray, value : int);\n");
//		pigServer.registerQuery("B = filter A by name eq 'UNKNOWN';\n");
//		pigServer
//				.registerQuery("C = load "
//						+ SpecClass
//						+ " using PigStorage() as (specObjID : long, SpecClass : int);\n");
//		pigServer.registerQuery("D = join B by value, C by SpecClass;\n");
//		pigServer.registerQuery("E = foreach D generate specObjID;\n");
//
//		Map<Operator, DataBag> derivedData = pigServer.getExamples("E");
//
//		assertTrue(derivedData != null);
//	}//
//	
//	/**
//	 * -- Find all galaxies with blue surface brightness between 23 and 25
//		-- mag per square arcseconds, and -10 < supergalactic latitude (sgb) < 10, and
//		-- declination less than zero. Currently, we have to live with ra/dec until we
//		-- get galactic coordinates. To calculate surface brightness per sq. arcsec,
//		-- we use (g + rho), where g is the blue magnitude, and rho= 5*log(r). This
//		-- query now has three requirements, one involving simple math.
//		
//		SELECT objID
//		FROM Galaxy
//		WHERE ra between 250 and 270
//		    and dec > 50 
//		    and (g+rho) between 23 and 25 	-- g is blue magnitude, and rho= 5*log(r)
//		    
//	* SELECT top 10 objID, ra, dec, g, rho  
//		FROM Galaxy 
//		
//	*	objID	bigint
//	    ra	float
//	    dec	float
//	    g	real
//	    rho	real
//
//	 * @throws Exception
//	 */
//	@Test
//	public void testMultipleCriteria() throws Exception {
//		System.out.println("testMultipleCriteria");
//
//		  PigServer pigServer = new PigServer(pigContext);
//
//	      pigServer.registerQuery("A = load " + Galaxy2
//	              + " using PigStorage() as (objID : long, ra : float, dec : float, g : float, rho : float);");
//	      pigServer.registerQuery("B = filter A by ra <= 270.0 and  ra >= 250.0 and dec > 50.0 and g+rho >= 23.0 and g+rho <= 25.0 ;");
//	      Map<Operator, DataBag> derivedData = pigServer.getExamples("B");
//
//	      assertTrue(derivedData != null);
//	}
//	
//	/**
//	 * -- Find galaxies in a given area of the sky, using a coordinate cut
//		-- in the unit vector cx,cy,cz that corresponds to RA beteen 40 and 100.
//		-- Another simple query that uses math in the WHERE clause.
//		
//		SELECT colc_g, colc_r
//		FROM Galaxy
//		WHERE (-0.642788 * cx + 0.766044 * cy>=0)
//		    and (-0.984808 * cx - 0.173648 * cy <0) 
//		    
//		* SELECT TOP 10 colc_g, colc_r, cx, cy
//			FROM  Galaxy
//			
//		* colc_g	real
//		* colc_r	real
//		* cx	float
//		* cy	float
//		
//		
//	 * @throws Exception
//	 */
//	@Test
//	public void testSpatialUnitVectors() throws Exception {
//		System.out.println("testSpatialUnitVectors");
//
//		  PigServer pigServer = new PigServer(pigContext);
//
//	      pigServer.registerQuery("A = load " + Galaxy3
//	              + " using PigStorage() as (colc_g : float, colc_r : float, cx : float, cy : float);");
//	      pigServer.registerQuery("B = filter A by  (-0.642788 * cx + 0.766044 * cy>=0.0) and (-0.984808 * cx - 0.173648 * cy <0.0) ;");
//	      Map<Operator, DataBag> derivedData = pigServer.getExamples("B");
//
//	      assertTrue(derivedData != null);
//	}
	
	/**
	 * -- Search for Cataclysmic Variables and pre-CVs with White Dwarfs and
		-- very late secondaries. Just uses some simple color cuts from Paula Szkody.
		-- Another simple query that uses math in the WHERE clause
		
		SELECT run,
		    camCol, 
		    rerun, 
		    field, 
		    objID, 
		    u, g, r, i, z, 
		    ra, dec 	-- Just get some basic quantities
		FROM PhotoPrimary		-- From all primary detections, regardless of class
		WHERE u - g < 0.4
		    and g - r < 0.7 
		    and r - i > 0.4 
		    and i - z > 0.4 	-- that meet the color criteria
		    
	*  SELECT TOP 10 run,         
		  camCol,
		  rerun,
		  field,
		  objID,
		  u, g, r, i, z,
		  ra, dec   		-- Just get some basic quantities
		FROM PhotoPrimary	-- From all primary detections, regardless of class
		
	*  run	smallint
	*  camcol	tinyint
	*  rerun	smallint
	*  field	smallint
	*  objID	bigint
	*  u	real
	*  g	real
	*  r	real
	*  i	real
	*  z	real
	*  ra	float
	*  dec	float	

	 * @throws Exception
	 */
	@Test
	public void testCVs() throws Exception {
		System.out.println("testCVs");

		PigServer pigServer = new PigServer(pigContext);

	    pigServer.registerQuery("A = load " + PhotoPrimary
	              + " using PigStorage() as (run : int, camcol : int, rerun : int, field : int, objID : long, u : double, g : double, r : double, i : double, z : double, ra : float, dec : float);");
	    pigServer.registerQuery("B = filter A by  u - g < 0.4 and g - r < 0.7 and r - i > 0.4 and i - z > 0.4 ;");
	    Map<Operator, DataBag> derivedData = pigServer.getExamples2("B");

	    assertTrue(derivedData != null);
	}
	
	/**
	 * -- Low-z QSO candidates using the color cuts from Gordon Richards.
		-- Also a simple query with a long WHERE clause.
		
		SELECT
		    g, 
		    run, 
		    rerun, 
		    camcol, 
		    field, 
		    objID 
		FROM
		    Galaxy 
		WHERE ( (g <= 22)
		    and (u - g >= -0.27) 
		    and (u - g < 0.71) 
		    and (g - r >= -0.24) 
		    and (g - r < 0.35) 
		    and (r - i >= -0.27) 
		    and (r - i < 0.57) 
		    and (i - z >= -0.35) 
		    and (i - z < 0.70) ) 
		    
		* SELECT TOP 10 g, run, rerun, camcol, field, objID, u, r, i, z
			FROM Galaxy 
			
			
		*  g	real
		*  run	smallint
		*  rerun	smallint
		*  camcol	tinyint
		*  field	smallint
		*  objID	bigint
		*  u	real
		*  r	real 
		*  i	real
		*  z	real
		
	 * @throws Exception
	 */
//	@Test
//	public void testLowzQSOs() throws Exception {
//		System.out.println("testLowzQSOs");
//
//		PigServer pigServer = new PigServer(pigContext);
//
//		pigServer.registerQuery("A = load " + Galaxy4
//	              + " using PigStorage() as ( g : double, run : int, rerun : int, camcol : int, field : int, objID : int, u : double, r : double, i : double, z : double);");
//	    pigServer.registerQuery("B = filter A by   ( (g <= 22.0) and (u - g >= -0.27) and (u - g < 0.71) and (g - r >= -0.24) and (g - r < 0.35) and (r - i >= -0.27) and (r - i < 0.57) and (i - z >= -0.35) and (i - z < 0.70) )  ;");
//	    Map<Operator, DataBag> derivedData = pigServer.getExamples("B");
//
//	    assertTrue(derivedData != null);
//	}
//	
//	/**
//	 * -- Get object velocities and errors. This is also a simple query that uses a WHERE clause.
//	-- However, we perform a more complex mathematical operation, using 'power' to
//	-- exponentiate. (From Robert Lupton).
//	
//	-- NOTE: This query takes a long time to run without the "TOP 1000".
//	
//	* SELECT TOP 1000
//	    run, 
//	    camCol, 
//	    field, 
//	    objID, 
//	    rowC, colC, rowV, colV, rowVErr, colVErr, 
//	    flags, 
//	    psfMag_u, psfMag_g, psfMag_r, psfMag_i, psfMag_z, 
//	    psfMagErr_u, psfMagErr_g, psfMagErr_r, psfMagErr_i, psfMagErr_z 
//	FROM PhotoPrimary
//	WHERE
//	    -- where the velocities are reliable 
//	    power(rowv, 2) / power(rowvErr, 2) + 
//	    power(colv, 2) / power(colvErr, 2) > 4 
//	    
//	    * SELECT TOP 10 
//		  run,	   -- get a bunch of quantities
//		  camCol,
//		  field,
//		  objID,
//		  rowC, colC, rowV, colV, rowvErr, colvErr,
//		  flags,
//		  psfMag_u, psfMag_g, psfMag_r, psfMag_i, psfMag_z,
//		  psfMagErr_u, psfMagErr_g, psfMagErr_r, psfMagErr_i, psfMagErr_z
//		FROM PhotoPrimary
//		* run	smallint,
//		  camCol	tinyint,
//		  field	smallint,	
//		  objID	bigint,
//		  rowC	real, 
//		  colC	real, 
//		  rowV	real, 
//		  colV	real, 
//		  rowvErr	real, 
//		  colvErr	real,
//		  flags	bigint,
//		  psfMag_u	real, 
//		  psfMag_g	real, 
//		  psfMag_r	real, 
//		  psfMag_i	real, 
//		  psfMag_z	real,
//		  psfMagErr_u	real, 
//		  psfMagErr_g	real, 
//		  psfMagErr_r	real, 
//		  psfMagErr_i	real, 
//		  psfMagErr_z	real
//	 * @throws Exception
//	 */
//	@Test
//	public void testVelocitiesErrors() throws Exception {
//		System.out.println("testVelocitiesErrors");
//
//		PigServer pigServer = new PigServer(pigContext);
//
//		pigServer.registerQuery("A = load " + PhotoPrimary2
//	              + " using PigStorage() as (run : int, camCol : int, field : int,	objID : long, rowC : double, colC : double, rowV : double, colV	: double, rowvErr : double, colvErr	: double, flags	: long, psfMag_u : double,  psfMag_g : double, psfMag_r	: double,  psfMag_i	: double, psfMag_z	: double, psfMagErr_u : double,  psfMagErr_g : double, psfMagErr_r	: double, psfMagErr_i : double, psfMagErr_z	: double);");
//	    pigServer.registerQuery("B = filter A by org.apache.pig.piggybank.evaluation.math.POW(rowV, 2.0) / org.apache.pig.piggybank.evaluation.math.POW(rowvErr, 2.0) + org.apache.pig.piggybank.evaluation.math.POW(colV, 2.0) / org.apache.pig.piggybank.evaluation.math.POW(colvErr, 2.0) > 4.0 ;");
//	    Map<Operator, DataBag> derivedData = pigServer.getExamples("B");
//
//	    assertTrue(derivedData != null);
//	}
	/*
	*//**
	 * -- Find galaxies with an isophotal surface brightness (SB) larger
		-- than 24 in the red band, and with an ellipticity > 0.5, and with the major
		-- axis of the ellipse having a declination between 30" and 60" arc seconds.
		-- This is also a simple query that uses a WHERE clause with three conditions
		-- that must be met. We introduce the syntax 'between' to do a range search.
		
		SELECT TOP 10 objID, r, rho, isoA_r
		FROM Galaxy
		WHERE
		    r + rho < 24 	-- red surface brightness more than
			-- 24 mag/sq-arcsec
		    and isoA_r between 30/0.4 and 60/0.4 	-- major axis between 30" and 60"
			-- (SDSS pixels = 0.4 arcsec)
		    and (power(q_r,2) + power(u_r,2)) > 0.25 	-- square of ellipticity > 0.5 squared 
		    
	 * SELECT TOP 10 ObjID, r, rho, isoA_r, q_r, u_r
		FROM Galaxy
		
	 * ObjID	bigint, 
	 * r	real, 
	 * rho	real, 
	 * isoA_r real, 
	 * q_r	real, 
	 * u_r	real
	 * @throws Exception
	 *//*
	@Test
	public void testBETWEEN() throws Exception {
		System.out.println("testBETWEEN");

		PigServer pigServer = new PigServer(pigContext);

		pigServer.registerQuery("A = load " + Galaxy5
	              + " using PigStorage() as (ObjID : long, r : double, rho : double, isoA_r : double, q_r : double, u_r : double);");
	    pigServer.registerQuery("B = filter A by r + rho < 24.0 and  isoA_r >= 75.0 and isoA_r <= 150.0 and  (org.apache.pig.piggybank.evaluation.math.POW(q_r,2.0) + org.apache.pig.piggybank.evaluation.math.POW(u_r,2.0)) > 0.25;");
	    Map<Operator, DataBag> derivedData = pigServer.getExamples("B");

	    assertTrue(derivedData != null);
	}
	
	*//**
	 * -- Provide a list of moving objects consistent with an asteroid.
	-- Also a simple query, but we introduce the 'as' syntax, which allows us to
	-- name derived quantities in the result file.
	
	SELECT
	    objID, 
	    sqrt( power(rowv,2) + power(colv, 2) ) as velocity 
	FROM PhotoObj
	WHERE
    (power(rowv,2) + power(colv, 2)) > 50 
    and rowv >= 0 and colv >=0 
    
    * SELECT TOP 10
  		objID, rowv, colv
		FROM PhotoObj 
		
	* objID	bigint
	* rowv	real
	* colV	real
	* 
	 * @throws Exception
	 *//*
	@Test
	public void testMovingAsteroids() throws Exception {
		System.out.println("testMovingAsteroids");

		  PigServer pigServer = new PigServer(pigContext);

		  pigServer.registerQuery("A = load " + PhotoObj2
	              + " using PigStorage() as (objID : long, rowv : double, colv : double);");
	      pigServer.registerQuery("B = filter A by  (org.apache.pig.piggybank.evaluation.math.POW(rowv,2.0) + org.apache.pig.piggybank.evaluation.math.POW(colv, 2.0)) > 50.0 and rowv >= 0.0 and colv >=0.0 ;");
	      pigServer.registerQuery("C = foreach B generate org.apache.pig.piggybank.evaluation.math.SQRT(org.apache.pig.piggybank.evaluation.math.POW(rowv,2.0) + org.apache.pig.piggybank.evaluation.math.POW(colv, 2.0)) as velocity;");

	      Map<Operator, DataBag> derivedData = pigServer.getExamples("C");

	      assertTrue(derivedData != null);
	}
	
*/
}
