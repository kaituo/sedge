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
import org.apache.pig.test.SessionIdentifierGenerator;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestExGen4Distinct {
	static PigContext pigContext = new PigContext(ExecType.LOCAL, new Properties());

	   
    static int MAX = 100;
    static String A, B, C;
    static  File fileA, fileB, fileC;
    
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

        writeData(fileA);
        writeData(fileB);
        writeData2(fileC);
     

        fileA.deleteOnExit();
        fileB.deleteOnExit();
        fileC.deleteOnExit();
        A = "'" + fileA.getPath() + "'";
        B = "'" + fileB.getPath() + "'";
        C = "'" + fileC.getPath() + "'";
        System.out.println("A : " + A + "\n" + "B : " + B + "\n" + "C : " + C);
        System.out.println("Test data created.");
    }

    private static void writeData(File dataFile) throws Exception {
        // File dataFile = File.createTempFile(name, ".dat");
        FileOutputStream dat = new FileOutputStream(dataFile);

        Random rand = new Random();

        for (int i = 0; i < MAX; i++)
            dat.write((rand.nextInt(10) + "\t" + rand.nextInt(10) + "\n")
                    .getBytes());

        dat.close();
    }
    
    private static void writeData2(File dataFile) throws Exception {
        // File dataFile = File.createTempFile(name, ".dat");
        FileOutputStream dat = new FileOutputStream(dataFile);

        Random rand = new Random();
        Integer a = rand.nextInt(10);
        Integer b = rand.nextInt(10);

        for (int i = 0; i < MAX; i++)
            dat.write(( a++ + "\t" + b++ + "\n")
                    .getBytes());

        dat.close();
    }
	
	 @Test
	    public void testDistinct() throws Exception {
	        PigServer pigServer = new PigServer(pigContext);
	        pigServer.registerQuery("A = load " + A.toString() + " using PigStorage() as (x : int, y : int);\n");
	        pigServer.registerQuery("B = DISTINCT A;");
	        Map<Operator, DataBag> derivedData = pigServer.getExamples2("B");

	        assertTrue(derivedData != null);
	    }
	 
	 @Test
	    public void testDistinct2() throws Exception {
	        PigServer pigServer = new PigServer(pigContext);
	        pigServer.registerQuery("A = load " + C.toString() + " using PigStorage() as (x : int, y : int);\n");
	        pigServer.registerQuery("B = DISTINCT A;");
	        Map<Operator, DataBag> derivedData = pigServer.getExamples2("B");

	        assertTrue(derivedData != null);
	    }

}
