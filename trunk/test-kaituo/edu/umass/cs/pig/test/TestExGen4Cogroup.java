package edu.umass.cs.pig.test;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
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

public class TestExGen4Cogroup {
	static PigContext pigContext = new PigContext(ExecType.LOCAL,
			new Properties());

	// private OSValidator z3 = OSValidator.get();
	// private Z3Context z4 = Z3Context.get();
	static int MAX = 100;
	static int MIN = 10;
	static String A, B, C, D, E, F, G;
	static int start = 0;
	static File fileA, fileB, fileC, fileD, fileE, fileF, fileG;

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
		System.out.println("A : " + A + "\n" + "B : " + B + "\n" + "C : " + C
				+ "\n" + "D : " + D + "\n" + "E : " + E + "\n" + "F : " + F + "\n" + "G : " + G + "\n");
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
			dat.write((start++ + "\t" + start++ + "\n").getBytes());

		dat.close();
	}
	
	private static void writeData4(File dataFile) throws Exception {
		// File dataFile = File.createTempFile(name, ".dat");
		FileOutputStream dat = new FileOutputStream(dataFile);

		Random rand = new Random();

		for (int i = 0; i < 3; i++)
			dat.write((3 + "\t" + rand.nextInt(10) + "\n")
				.getBytes());
		
		for (int i = 0; i < MIN; i++)
			dat.write((rand.nextInt(10) + "\t" + rand.nextInt(10) + "\n")
					.getBytes());

		dat.close();
	}
	
	private static void writeData5(File dataFile) throws Exception {
		// File dataFile = File.createTempFile(name, ".dat");
		FileOutputStream dat = new FileOutputStream(dataFile);

		Random rand = new Random();

		for (int i = 0; i < 3; i++)
			dat.write((3 + "\t" + rand.nextInt(10) + "\n")
				.getBytes());
		
		for (int i = 0; i < MIN; i++)
			dat.write((rand.nextInt(10) + "\t" + rand.nextInt(10) + "\n")
					.getBytes());

		dat.close();
	}

	@Test
	public void testCogroupMultipleCols() throws Exception {

		PigServer pigServer = new PigServer(pigContext);
		pigServer.registerQuery("A = load " + A + " using PigStorage() as (x : int, y : int);\n");
		pigServer.registerQuery("B = load " + B + " using PigStorage() as (x : int, y : int);\n");
		pigServer.registerQuery("C = cogroup A by (x, y), B by (x, y);");
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("C");

		assertTrue(derivedData != null);
	}

	@Test
	public void testCogroup() throws Exception {
		PigServer pigServer = new PigServer(pigContext);
		pigServer.registerQuery("A = load " + A + " using PigStorage() as (x : int, y : int);\n");
		pigServer.registerQuery("B = load " + B + " using PigStorage() as (x : int, y : int);\n");
		pigServer.registerQuery("C = cogroup A by x, B by x;");
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("C");

		assertTrue(derivedData != null);
	}

	@Test
	public void testCogroup2() throws Exception {
		PigServer pigServer = new PigServer(pigContext);
		pigServer.registerQuery("A = load " + D + " using PigStorage() as (x : int, y : int);\n");
		pigServer.registerQuery("B = load " + E + " using PigStorage() as (x : int, y : int);\n");
		pigServer.registerQuery("C = cogroup A by x, B by x;");
		// pigServer.registerQuery("D = filter C by A::x > 9 ;");
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("C");

		assertTrue(derivedData != null);
	}
	
	@Test
	public void testCogroup3() throws Exception {
		File out = File.createTempFile("testCogroup3", "");
	    out.deleteOnExit();
	    out.delete();
	      
		PigServer pigServer = new PigServer(pigContext);
		pigServer.setBatchOn();
		
		pigServer.registerQuery("A = load " + D + " using PigStorage() as (x : int, y : int);\n");
		pigServer.registerQuery("B = load " + E + " using PigStorage() as (x : int, y : int);\n");
		pigServer.registerQuery("C = cogroup A by x, B by x;");
		pigServer.registerQuery("store C into '" +  out.getAbsolutePath() + "';");
		Map<Operator, DataBag> derivedData = pigServer.getExamples2(null);

		assertTrue(derivedData != null);
	}
	
	@Test
	public void testCogroup4() throws Exception {
		PigServer pigServer = new PigServer(pigContext);
		pigServer.registerQuery("A = load " + F + " using PigStorage() as (x : int, y : int);\n");
		pigServer.registerQuery("B = load " + G + " using PigStorage() as (x : int, y : int);\n");
		pigServer.registerQuery("C = cogroup A by x, B by x;");
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("C");

		assertTrue(derivedData != null);
	}
	
	@Test
	public void testCogroup5() throws Exception {
		PigServer pigServer = new PigServer(pigContext);
		pigServer.registerQuery("A = load " + F + " using PigStorage() as (x : int, y : int);\n");
		pigServer.registerQuery("B = load " + F + " using PigStorage() as (x : int, y : int);\n");
		pigServer.registerQuery("C = cogroup A by x, B by x;");
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("C");

		assertTrue(derivedData != null);
	}

}
