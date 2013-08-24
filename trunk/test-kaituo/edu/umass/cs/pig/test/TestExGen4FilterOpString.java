package edu.umass.cs.pig.test;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.newplan.Operator;
import org.apache.pig.test.MiniCluster;
import org.apache.pig.test.Util;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestExGen4FilterOpString extends TestCase {
	private final Log log = LogFactory.getLog(getClass());
    private static int LOOP_COUNT = 1024;    
    private static MiniCluster cluster = MiniCluster.buildCluster();

    static PigContext pigContext = new PigContext(ExecType.LOCAL, new Properties());
    
    {
        try {
            pigContext.connect();
        } catch (ExecException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    private PigServer pig;
    
    @Before
    @Override
    public void setUp() throws Exception {
        FileLocalizer.deleteTempFiles();
        pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }
    
    @Test
    public void testStringEq() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        tmpFile.deleteOnExit();
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        int expectedCount = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println("a:" + i);
                // test with nulls
                ps.println("a:");
                ps.println(":a");
                ps.println(":");
            } else {
                ps.println("ab:ba");
                expectedCount++;
            }
        }
        ps.close();
        pig.registerQuery("A=load '" 
                + Util.generateURI(tmpFile.toString(), pig.getPigContext()) 
                + "' using " + PigStorage.class.getName() + "(':') as (x : chararray, y : chararray);");
        String query = "B = filter A by x eq y;";

        log.info(query);
        pig.registerQuery(query);
        Map<Operator, DataBag> derivedData = pig.getExamples2("B");

        assertTrue(derivedData != null);
        
    }
    
    @Test
    public void testStringEq2() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        tmpFile.deleteOnExit();
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        int expectedCount = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println("a:" + i);
                // test with nulls
                ps.println("a:");
                ps.println(":a");
                ps.println(":");
            } else {
                ps.println("ab:ba");
                expectedCount++;
            }
        }
        ps.close();
        pig.registerQuery("A=load '" 
                + Util.generateURI(tmpFile.toString(), pig.getPigContext()) 
                + "' using " + PigStorage.class.getName() + "(':') as (x : chararray, y : chararray);");
        String query = "B = filter A by x eq 'ca';";

        log.info(query);
        pig.registerQuery(query);
        Map<Operator, DataBag> derivedData = pig.getExamples2("B");

        assertTrue(derivedData != null);
        
    }
    
    @Test
    public void testStringNeq() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        tmpFile.deleteOnExit();
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        int expectedCount = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println("ab:ab");
            } else if (i % 3 == 0) {
                ps.println("abc:abc");
                expectedCount++;
            } else {
                // test with nulls
                ps.println(":");
                ps.println("ab:");
                ps.println(":ab");                
            }
        }
        ps.close();
        pig.registerQuery("A=load '" 
                + Util.generateURI(tmpFile.toString(), pig.getPigContext()) 
                + "' using " + PigStorage.class.getName() + "(':') as (x : chararray, y : chararray);");
        String query = "B = filter A by x neq y;";

        log.info(query);
        pig.registerQuery(query);
        Map<Operator, DataBag> derivedData = pig.getExamples2("B");

        assertTrue(derivedData != null);
    }
    
    @Test
    public void testStringNeq2() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        tmpFile.deleteOnExit();
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        int expectedCount = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println("ab:ab");
            } else if (i % 3 == 0) {
                ps.println("ab:abc");
                expectedCount++;
            } else {
                // test with nulls
                ps.println(":");
                ps.println("ab:");
                ps.println(":ab");                
            }
        }
        ps.close();
        pig.registerQuery("A=load '" 
                + Util.generateURI(tmpFile.toString(), pig.getPigContext()) 
                + "' using " + PigStorage.class.getName() + "(':') as (x : chararray, y : chararray);");
        String query = "B = filter A by x neq 'nbc';";

        log.info(query);
        pig.registerQuery(query);
        Map<Operator, DataBag> derivedData = pig.getExamples2("B");

        assertTrue(derivedData != null);
    }

    @Test
    public void testStringGt() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        tmpFile.deleteOnExit();
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        int expectedCount = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println("b:a");
                expectedCount++;
            } else {
                ps.println("a:b");
                // test with nulls
                ps.println("a:");
                ps.println(":b");
                ps.println(":");
                
            }
        }
        ps.close();
        pig.registerQuery("A=load '" 
                + Util.generateURI(tmpFile.toString(), pig.getPigContext()) 
                + "' using " + PigStorage.class.getName() + "(':') as (x : chararray, y : chararray);");
        String query = "B = filter A by x gt y;";

        log.info(query);
        pig.registerQuery(query);
        Map<Operator, DataBag> derivedData = pig.getExamples2("B");

        assertTrue(derivedData != null);
    }
    
    @Test
    public void testStringGt2() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        tmpFile.deleteOnExit();
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        int expectedCount = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println("b:b");
                expectedCount++;
            } else {
                ps.println("a:b");
                // test with nulls
                ps.println("a:");
                ps.println(":b");
                ps.println(":");
                
            }
        }
        ps.close();
        pig.registerQuery("A=load '" 
                + Util.generateURI(tmpFile.toString(), pig.getPigContext()) 
                + "' using " + PigStorage.class.getName() + "(':') as (x : chararray, y : chararray);");
        String query = "B = filter A by x gt y;";

        log.info(query);
        pig.registerQuery(query);
        Map<Operator, DataBag> derivedData = pig.getExamples2("B");

        assertTrue(derivedData != null);
    }
    
    @Test
    public void testStringGt3() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        tmpFile.deleteOnExit();
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        int expectedCount = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println("b:a");
                expectedCount++;
            } else {
                ps.println("a:b");
                // test with nulls
                ps.println("a:");
                ps.println(":b");
                ps.println(":");
                
            }
        }
        ps.close();
        pig.registerQuery("A=load '" 
                + Util.generateURI(tmpFile.toString(), pig.getPigContext()) 
                + "' using " + PigStorage.class.getName() + "(':') as (x : chararray, y : chararray);");
        String query = "B = filter A by x gt 'cd';";

        log.info(query);
        pig.registerQuery(query);
        Map<Operator, DataBag> derivedData = pig.getExamples2("B");

        assertTrue(derivedData != null);
    }

    

    @Test
    public void testStringGte() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        tmpFile.deleteOnExit();
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        int expectedCount = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println("b:a");
                expectedCount++;
            }else if(i % 3 == 0) {
                ps.println("b:b");
                expectedCount++;
            } else {
                ps.println("a:b");
                // test with nulls
                ps.println("a:");
                ps.println(":b");
                ps.println(":");
            }
        }
        ps.close();
        
        pig.registerQuery("A=load '" 
                + Util.generateURI(tmpFile.toString(), pig.getPigContext()) 
                + "' using " + PigStorage.class.getName() + "(':') as (x : chararray, y : chararray);");
        String query = "B = filter A by x gte y;";

        log.info(query);
        pig.registerQuery(query);
        Map<Operator, DataBag> derivedData = pig.getExamples2("B");

        assertTrue(derivedData != null);
    }

    @Test
    public void testStringLt() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        tmpFile.deleteOnExit();
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        int expectedCount = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println("b:a");
                // test with nulls
                ps.println("a:");
                ps.println(":b");
                ps.println(":");
            } else {
                ps.println("a:b");
                expectedCount++;
            }
        }
        ps.close();
        
        pig.registerQuery("A=load '" 
                + Util.generateURI(tmpFile.toString(), pig.getPigContext()) 
                + "' using " + PigStorage.class.getName() + "(':') as (x : chararray, y : chararray);");
        String query = "B = filter A by x lt y;";

        Map<Operator, DataBag> derivedData = pig.getExamples2("B");

        assertTrue(derivedData != null);
    }

    @Test
    public void testStringLte() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        tmpFile.deleteOnExit();
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        int expectedCount = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println("b:a");
                // test with nulls
                ps.println("a:");
                ps.println(":b");
                ps.println(":");
            }else if(i % 3 == 0) {
                ps.println("b:b");
                expectedCount++;
            } else {
                ps.println("a:b");
                expectedCount++;
            }
        }
        ps.close();
        
        pig.registerQuery("A=load '" 
                + Util.generateURI(tmpFile.toString(), pig.getPigContext()) 
                + "' using " + PigStorage.class.getName() + "(':') as (x : chararray, y : chararray);");
        String query = "B = filter A by x lte y;";

        log.info(query);
        pig.registerQuery(query);
        Map<Operator, DataBag> derivedData = pig.getExamples2("B");

        assertTrue(derivedData != null);
    }

}
