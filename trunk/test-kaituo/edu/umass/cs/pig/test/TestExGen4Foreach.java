package edu.umass.cs.pig.test;

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
import org.apache.pig.test.SessionIdentifierGenerator;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestExGen4Foreach {
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
    public void testForeach() throws Exception {

        PigServer pigserver = new PigServer(pigContext);

        String query = "A = load " + A
                + " using PigStorage() as (x : int, y : int);\n";
        pigserver.registerQuery(query);
        query = "B = filter A by x * y > 200;";
        pigserver.registerQuery(query);
        query = "C = FOREACH B GENERATE (x+1) as x1, (y+1) as y1;";
        pigserver.registerQuery(query);
        query = "D = FOREACH C GENERATE (x1+1) as x2, (y1+1) as y2;";
        pigserver.registerQuery(query);
        query = "E = DISTINCT D;";
        pigserver.registerQuery(query);
        Map<Operator, DataBag> derivedData = pigserver.getExamples2("E");
        assertTrue(derivedData != null);
    }
    
    @Test
    public void testForeach2() throws ExecException, IOException {
        PigServer pigServer = new PigServer(pigContext);

        pigServer.registerQuery("A = load " + A
                + " using PigStorage() as (x : int, y : int);");
        pigServer.registerQuery("B = foreach A generate x + y as sum;");

        Map<Operator, DataBag> derivedData = pigServer.getExamples2("B");

        assertTrue(derivedData != null);
    }
    
    @Test
    public void testForeachBinCondWithBooleanExp() throws ExecException, IOException {
        PigServer pigServer = new PigServer(pigContext);

        pigServer.registerQuery("A = load " + A
                + " using PigStorage() as (x : int, y : int);");
        pigServer.registerQuery("B = foreach A generate  (x + 1 > y ? 0 : 1);");

        Map<Operator, DataBag> derivedData = pigServer.getExamples2("B");

        assertTrue(derivedData != null);
    }
    
    @Test
    public void testForeachWithTypeCastCounter() throws ExecException, IOException {
        PigServer pigServer = new PigServer(pigContext);
        //cast error results in counter being incremented and was resulting
        // in a NPE exception in illustrate
        pigServer.registerQuery("A = load " + A
                + " using PigStorage() as (x : int, y : int);");
        pigServer.registerQuery("B = foreach A generate x, (int)'InvalidInt';");

        Map<Operator, DataBag> derivedData = pigServer.getExamples("B");

        assertTrue(derivedData != null);
    }
    
    @Test
    public void testForeach3() throws Exception {

        PigServer pigserver = new PigServer(pigContext);

        String query = "A = load " + A
                + " using PigStorage() as (x : int, y : int);\n";
        pigserver.registerQuery(query);
        
        query = "B = FOREACH A GENERATE (x+1) as x1, (y+1) as y1;";
        pigserver.registerQuery(query);
        
        query = "C = filter B by x1 * y1 > 200;";
        pigserver.registerQuery(query);
        
        query = "D = FOREACH C GENERATE (x1+1) as x2, (y1+1) as y2;";
        pigserver.registerQuery(query);
        query = "E = DISTINCT D;";
        pigserver.registerQuery(query);
        Map<Operator, DataBag> derivedData = pigserver.getExamples2("E");
        assertTrue(derivedData != null);
    }
}
