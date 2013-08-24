package org.apache.pig.test;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
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

public class TestExampleGenerator2 {

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

        SessionIdentifierGenerator rand = new SessionIdentifierGenerator();
        Random rand2 = new Random();

        for (int i = 0; i < MAX; i++)
            dat.write((rand.nextSessionId() + "\t" + rand2.nextInt(10) + "\n")
                    .getBytes());

        dat.close();
    }

    @Test
    public void testLoad() throws Exception {

        PigServer pigserver = new PigServer(pigContext);

        String query = "A = load " + A
                + " using PigStorage() as (x : int, y : int);\n";
        pigserver.registerQuery(query);
        Map<Operator, DataBag> derivedData = pigserver.getExamples2("A");

        assertTrue(derivedData != null);
    }

    @Test
    public void testCogroupMultipleCols() throws Exception {

        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A = load " + A + " as (x, y);");
        pigServer.registerQuery("B = load " + B + " as (x, y);");
        pigServer.registerQuery("C = cogroup A by (x, y), B by (x, y);");
        Map<Operator, DataBag> derivedData = pigServer.getExamples("C");

        assertTrue(derivedData != null);
    }

    @Test
    public void testCogroup() throws Exception {
        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A = load " + A + " as (x, y);");
        pigServer.registerQuery("B = load " + B + " as (x, y);");
        pigServer.registerQuery("C = cogroup A by x, B by x;");
        Map<Operator, DataBag> derivedData = pigServer.getExamples("C");

        assertTrue(derivedData != null);
    }

    @Test
    public void testGroup() throws Exception {
        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A = load " + A.toString() + " as (x, y);");
        pigServer.registerQuery("B = group A by x;");
        Map<Operator, DataBag> derivedData = pigServer.getExamples("B");

        assertTrue(derivedData != null);

    }
    
    @Test
    public void testGroup2() throws Exception {
        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A = load " + A.toString() + " as (x:int, y:int);");
        pigServer.registerQuery("B = group A by x;");
        pigServer.registerQuery("C = foreach B generate group, COUNT(A);");
        Map<Operator, DataBag> derivedData = pigServer.getExamples("C");

        assertTrue(derivedData != null);

    }

    @Test
    public void testGroup3() throws Exception {
        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A = load " + A.toString() + " as (x:int, y:int);");
        pigServer.registerQuery("B = FILTER A by x  > 3;");
        pigServer.registerQuery("C = group B by y;");
        pigServer.registerQuery("D = foreach C generate group, COUNT(B);");
        Map<Operator, DataBag> derivedData = pigServer.getExamples("D");

        assertTrue(derivedData != null);

    }
    
}
