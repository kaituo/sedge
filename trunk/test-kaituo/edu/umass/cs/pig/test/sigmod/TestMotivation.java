package edu.umass.cs.pig.test.sigmod;

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

public class TestMotivation {
	static PigContext pigContext = new PigContext(ExecType.LOCAL, new Properties());

	   
    static int MAX = 1000;
    static String A, B, C, g1, g2, g3;
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

        writeData2(fileA);
        writeData(fileB);
        writeData3(fileC);
     

        fileA.deleteOnExit();
        fileB.deleteOnExit();
        fileC.deleteOnExit();
        A = "'" + fileA.getPath() + "'";
        B = "'" + fileB.getPath() + "'";
        C = "'" + fileC.getPath() + "'";
        System.out.println("A : " + A + "\n" + "B : " + B + "\n" + "C : " + C + "\n");
        System.out.println("Test data created.");
        
        g1 = "'" + "/home/kaituo/code/pig3/trunk/SDSSRandomData/EmptyFileFolder/part-m-00000" + "'";
		g2 = "'" + "/home/kaituo/code/pig3/trunk/SDSSRandomData/EmptyFileFolder/part-r-00000" + "'";
		g3 = "'" + "/home/kaituo/code/pig3/trunk/SDSSRandomData/EmptyFileFolder/part-q-00000" + "'";
    }
    
    private static void writeData3(File dataFile) throws Exception {
        // File dataFile = File.createTempFile(name, ".dat");
        FileOutputStream dat = new FileOutputStream(dataFile);

        Random rand = new Random();

        for (int i = 0; i < MAX; i++)
            dat.write((rand.nextInt() + "\n")
                    .getBytes());

        dat.close();
    }

    private static void writeData(File dataFile) throws Exception {
        // File dataFile = File.createTempFile(name, ".dat");
        FileOutputStream dat = new FileOutputStream(dataFile);

        Random rand = new Random();

        for (int i = 0; i < MAX; i++)
            dat.write((rand.nextInt() + "\t" + rand.nextInt() + "\n")
                    .getBytes());

        dat.close();
    }
    
    private static void writeData2(File dataFile) throws Exception {
        // File dataFile = File.createTempFile(name, ".dat");
        FileOutputStream dat = new FileOutputStream(dataFile);

        Random rand = new Random();

        for (int i = 0; i < MAX; i++)
            dat.write((rand.nextInt() + "\t" + rand.nextInt() + "\t" + rand.nextInt() + "\n")
                    .getBytes());

        dat.close();
    }
    
//    @Test
//    public void testFilter0() throws Exception {
//
//        PigServer pigserver = new PigServer(pigContext);
//
//        String query = "A = load " + A
//                + " using PigStorage() as (x : int, y : int, z : int);\n";
//        pigserver.registerQuery(query);
//        query = "B = load " + B 
//        		+ " using PigStorage() as (x : int, v : int);\n";
//        pigserver.registerQuery(query);
////        query = "C = filter A by x%2 == 0;\n";
////        pigserver.registerQuery(query);
//        query = "D = filter B by x < 0;\n";
//        pigserver.registerQuery(query);
//        query = "E = join A by x, D by x;\n";
//        pigserver.registerQuery(query);
//        
//        Map<Operator, DataBag> derivedData = pigserver.getExamples("E");
//        assertTrue(derivedData != null);
//    }
//    
//    @Test
//    public void testFilter1() throws Exception {
//
//        PigServer pigserver = new PigServer(pigContext);
//
//        String query = "A = load " + A
//                + " using PigStorage() as (x : int, y : int, z : int);\n";
//        pigserver.registerQuery(query);
//        query = "B = load " + B 
//        		+ " using PigStorage() as (x : int, v : int);\n";
//        pigserver.registerQuery(query);
//        query = "C = filter A by x%2 == 0;\n";
//        pigserver.registerQuery(query);
//        query = "D = filter B by x > 2;\n";
//        pigserver.registerQuery(query);
//        query = "E = join C by x, D by x;\n";
//        pigserver.registerQuery(query);
//        
//        Map<Operator, DataBag> derivedData = pigserver.getExamples2("E");
//        assertTrue(derivedData != null);
//    }
//    
//    @Test
//    public void testFilter2() throws Exception {
//
//        PigServer pigserver = new PigServer(pigContext);
//
//        String query = "A = load " + A
//                + " using PigStorage() as (x : int, y : int, z : int);\n";
//        pigserver.registerQuery(query);
//        query = "B = load " + B 
//        		+ " using PigStorage() as (x : int, v : int);\n";
//        pigserver.registerQuery(query);
//        query = "C = filter A by x%2 == 0;\n";
//        pigserver.registerQuery(query);
//        query = "D = join C by x, B by x;\n";
//        pigserver.registerQuery(query);
//        query = "E = filter D by C::x > 2;\n";
//        pigserver.registerQuery(query);
//        
//
//        Map<Operator, DataBag> derivedData = pigserver.getExamples2("E");
//        assertTrue(derivedData != null);
//    }
    
    @Test
    public void testDoubleJoin() throws Exception {
    	PigServer pigserver = new PigServer(pigContext);

    	String query = "A = load " + g1
                + " using PigStorage() as (x : int, x2 : int, x3 : int);\n";
        pigserver.registerQuery(query);
        query = "B = load " + g2 
        		+ " using PigStorage() as (y : int, y2 : int);\n";
        pigserver.registerQuery(query);
        query = "C = load " + g3 
        		+ " using PigStorage() as (z : int);\n";
        pigserver.registerQuery(query);
        query = "E = join A by x, B by y;\n";
        pigserver.registerQuery(query);
        query = "F = join E by y, C by z;\n";
        pigserver.registerQuery(query);
        query = "G = union F, E;\n";
        pigserver.registerQuery(query);
        

        Map<Operator, DataBag> derivedData = pigserver.getExamples2("G");
        assertTrue(derivedData != null);
    }

}
