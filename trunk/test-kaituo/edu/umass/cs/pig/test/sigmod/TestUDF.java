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
import org.junit.BeforeClass;
import org.junit.Test;

public class TestUDF {
	static PigContext pigContext = new PigContext(ExecType.LOCAL, new Properties());

	   
    static int MAX = 1000;
    static String A, B;
    static  File fileA, fileB;
    
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

        writeData(fileA);
        writeData2(fileB);
     

        fileA.deleteOnExit();
        fileB.deleteOnExit();
        A = "'" + fileA.getPath() + "'";
        B = "'" + fileB.getPath() + "'";
        System.out.println("A : " + A + "\n" + "B : " + B + "\n");
        System.out.println("Test data created.");
    }

    private static void writeData(File dataFile) throws Exception {
        // File dataFile = File.createTempFile(name, ".dat");
        FileOutputStream dat = new FileOutputStream(dataFile);

        Random rand = new Random();

        
        dat.write((33 + "\t" + 42 + "\n")
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
    
    @Test
    public void testFilter2() throws Exception {

        PigServer pigserver = new PigServer(pigContext);

        String query = "A = load " + A
                + " using PigStorage() as (x : int, y : int);\n";
        pigserver.registerQuery(query);
        query = "B = FILTER A BY x == edu.umass.cs.pig.test.sigmod.HASH(y);\n";
        pigserver.registerQuery(query);
        

        Map<Operator, DataBag> derivedData = pigserver.getExamples("B");
        assertTrue(derivedData != null);
    }

}
