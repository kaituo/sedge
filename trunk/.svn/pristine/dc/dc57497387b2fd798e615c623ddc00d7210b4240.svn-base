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

public class TestExGen4Group {
	static PigContext pigContext = new PigContext(ExecType.LOCAL,
			new Properties());

	// private OSValidator z3 = OSValidator.get();
	// private Z3Context z4 = Z3Context.get();
	static int MAX = 100;
	static int MIN = 10;
	static String A, B, C, D, E;
	static int start = 0;
	static File fileA, fileB, fileC, fileD, fileE;

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

		writeData(fileA);
		writeData(fileB);

		fileA.deleteOnExit();
		fileB.deleteOnExit();
		fileC.deleteOnExit();
		fileD.deleteOnExit();
		fileE.deleteOnExit();

		A = "'" + fileA.getPath() + "'";
		B = "'" + fileB.getPath() + "'";
		C = "'" + fileC.getPath() + "'";
		D = "'" + fileD.getPath() + "'";
		E = "'" + fileE.getPath() + "'";
		System.out.println("A : " + A + "\n" + "B : " + B + "\n" + "C : " + C
				+ "D : " + D + "E : " + E);
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

	@Test
	public void testGroup() throws Exception {
		PigServer pigServer = new PigServer(pigContext);
		pigServer.registerQuery("A = load " + A.toString() + " as (x, y);");
		pigServer.registerQuery("B = group A by x;");
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("B");

		assertTrue(derivedData != null);

	}

	@Test
	public void testGroup2() throws Exception {
		PigServer pigServer = new PigServer(pigContext);
		pigServer.registerQuery("A = load " + A.toString()
				+ " as (x:int, y:int);");
		pigServer.registerQuery("B = group A by x;");
		pigServer.registerQuery("C = foreach B generate group, COUNT(A);");
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("C");

		assertTrue(derivedData != null);

	}

	@Test
	public void testGroup3() throws Exception {
		PigServer pigServer = new PigServer(pigContext);
		pigServer.registerQuery("A = load " + A.toString()
				+ " as (x:int, y:int);");
		pigServer.registerQuery("B = FILTER A by x  > 13;");
		pigServer.registerQuery("C = group B by y;");
		pigServer.registerQuery("D = foreach C generate group, COUNT(B);");
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("D");

		assertTrue(derivedData != null);

	}

}
