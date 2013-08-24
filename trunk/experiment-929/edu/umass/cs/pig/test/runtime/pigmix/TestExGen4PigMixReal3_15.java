package edu.umass.cs.pig.test.runtime.pigmix;

import static org.junit.Assert.assertTrue;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.Operator;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.umass.cs.pig.test.util.RFile;
import edu.umass.cs.pig.test.util.StreamGobbler;

public class TestExGen4PigMixReal3_15 {
	static PigContext pigContext = new PigContext(ExecType.LOCAL, new Properties());
	static int MAX = 100;
	static int MIN = 10;
	static String widerowX, page_viewsX, power_usersX, usersX, data12X, users_sortedX, power_users_sampleX, widegroupbydataX;//widerow, 
//	static  File fileWiderowX;
//  page_views, , power_users
	static String pScalability;
// pPage_Views, , pPower_Users
	public static Logger logger = Logger.getLogger("MyLog");
	public static FileHandler fh;
	
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
		try {
			RFile.deleteIfExists("MyLogFile.log");
	        fh = new FileHandler("MyLogFile.log", true);
	        logger.addHandler(fh);
			logger.setLevel(Level.FINE);
			SimpleFormatter formatter = new SimpleFormatter();
			fh.setFormatter(formatter);
			fh.setLevel(Level.FINE);
        } catch (SecurityException e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
        } catch (IOException e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
        }
		
//		widerow = "/home/kaituo/code/pig3/trunk/PigmixRandomData/10/widerow/widerow10m0";
//		page_views = "'" + "/home/kaituo/code/pig3/trunk/pigmixdata/page_views" + "'";
//		power_users = "'" + "/home/kaituo/code/pig3/trunk/pigmixdata/power_users100m" + "'";
		
//		fileWiderowX = File.createTempFile("dataWiderowX", ".dat");
		
//		pScalability =  "/home/kaituo/code/pig3/trunk/scripts/pScalability.pig";
//		pPage_Views = "/home/kaituo/code/pig3/trunk/scripts/pPage_Views.pig";
//		pPower_Users = "/home/kaituo/code/pig3/trunk/scripts/pPower_Users.pig";
		
//		RFile.createRFile(pScalability);
//		RFile.createRFile(pPage_Views);
//		RFile.createRFile(pPower_Users);
		
//		widerowX = "'" + fileWiderowX.getPath() + "'";
//		widerowX = "/home/kaituo/code/pig3/trunk/PigmixRandomData/10/widerowX/scalabilityData0";
		page_viewsX = "/home/kaituo/code/pig3/trunk/PigmixRandomData/100/page_viewsX/pages625m9";
//		power_usersX = "/home/kaituo/code/pig3/trunk/PigmixRandomData/10/power_usersX/power_users100m0";
//		usersX = "/home/kaituo/code/pig3/trunk/PigmixRandomData/10/usersX/users100m0";
//		data12X = "/home/kaituo/code/pig3/trunk/PigmixRandomData/10/data12X/data12N0";
//		users_sortedX = "/home/kaituo/code/pig3/trunk/PigmixRandomData/10/data14X/data14N0";
//		power_users_sampleX = "/home/kaituo/code/pig3/trunk/PigmixRandomData/10/power_users_sampleX/power_users_sample0";
//		widegroupbydataX = "/home/kaituo/code/pig3/trunk/PigmixRandomData/10/widegroupbydataX/widegroupbydata0";
		
//		RFile.deleteIfExists(widerowX);
//		RFile.deleteIfExists(page_viewsX);
//		RFile.deleteIfExists(power_usersX);
		
//		writeScalabilityData(widerow, widerowX, pScalability);
//		writePage_ViewsData(page_views, page_viewsX, pPage_Views);

//		System.out.println("widerowX : " + widerowX + "\n" + "page_viewsX : " + page_viewsX + "\n" + "power_usersX : " + power_usersX + "\n" + "usersX: " + usersX + "\n" + "data12X: " + data12X + "\n" + "users_sortedX: " + users_sortedX + "\n"); 
//	    System.out.println("Test data created.");
	}
	
	private static void writeScalabilityData(String dataOrig, String dataOut, String PIG_FILE) {
		int n = 501;

		String abody = "";
		for (int i = 0; i < n; i++) {
			if (i > 0)
				abody = abody + ", ";
			abody = abody + "c" + i + ": int";
		}

//		String cbody = "";
//		for (int j = 0; j < n; j++) {
//			if (j > 0)
//				cbody = cbody + ", ";
//			cbody = cbody + "SUM(A.c" + j + ") as c" + j;
//		}
//
//		String dbody = "";
//		for (int k = 0; k < n; k++) {
//			if (k > 0)
//				dbody = dbody + " and ";
//			dbody = dbody + "c" + k + " > 100";
//		}
		
		try {
			PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
	        w.println("A = load '" + dataOrig.toString()
					+ "' using PigStorage('\u0001') as (name: chararray, " + abody + ");");
	        w.println("B = foreach A generate name, c0, c1;");
	        w.println("C = limit B " + MIN + ";");
	        w.println("store C into '" + dataOut + "';");
	        w.close();
	        
	        Process pp = Runtime.getRuntime().exec("java -Xmx256m -cp /home/kaituo/Downloads/pig-0.9.2/pig-0.9.2.jar org.apache.pig.Main -x local " + PIG_FILE.toString());
	        logErrororOutput(pp);
		} catch(IOException io) {
			io.printStackTrace();
		} 
		
	}
	
//	private static void writePage_ViewsData(String dataOrig, String dataOut, String PIG_FILE) {
//		try {
//			PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
//			w.println("register pigperf.jar;");
//	        w.println("A = load " + dataOrig.toString()
//					+ " using org.apache.pig.test.udf.storefunc.PigPerformanceLoader() as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);");  
//	        //as (user: chararray, action:int, timespent:int, query_term:chararray, ip_addr:long, timestamp:long, estimated_revenue:double, page_info:map[], page_links:bag{t:(p:map[])} );");
//	        w.println("B = limit A " + MIN + ";");
//	        w.println("store B into '" + dataOut + "' using PigStorage('\u0001');");
//	        w.close();
//	        
//	        Process pp = Runtime.getRuntime().exec("java -Xmx256m -cp /home/kaituo/Downloads/pig-0.9.2/pig-0.9.2.jar:/home/kaituo/code/pig3/trunk/pigperf.jar org.apache.pig.Main -x local " + PIG_FILE.toString());
//	        logErrororOutput(pp);
//		} catch(IOException io) {
//			io.printStackTrace();
//		} 
//		
//	}
	
//	private static void writePower_UsersData(String dataOrig, String dataOut, String PIG_FILE) {
//		try {
//			PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
//			w.println("register pigperf.jar;");
//	        w.println("A = load " + dataOrig.toString()
//					+ " using PigStorage('\u0001') as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);");  
//	        //as (user: chararray, action:int, timespent:int, query_term:chararray, ip_addr:long, timestamp:long, estimated_revenue:double, page_info:map[], page_links:bag{t:(p:map[])} );");
//	        w.println("B = limit A " + MIN + ";");
//	        w.println("store B into '" + dataOut + "' using PigStorage('\u0001');");
//	        w.close();
//	        
//	        Process pp = Runtime.getRuntime().exec("java -Xmx256m -cp /home/kaituo/Downloads/pig-0.9.2/pig-0.9.2.jar:/home/kaituo/code/pig3/trunk/pigperf.jar org.apache.pig.Main -x local " + PIG_FILE.toString());
//	        logErrororOutput(pp);
//		} catch(IOException io) {
//			io.printStackTrace();
//		} 
//	}
	
	private static void logErrororOutput(Process proc) {
        // any error message?
        StreamGobbler errorGobbler = new StreamGobbler(proc.getErrorStream(), "OUTPUT", logger);            
        
        // any output?
        StreamGobbler outputGobbler = new StreamGobbler(proc.getInputStream(), "OUTPUT", logger);
            
        // kick them off
        errorGobbler.start();
        outputGobbler.start();
                                
        // any error???
        int exitVal;
        try {
	        exitVal = proc.waitFor();
	        logger.log(Level.FINE, "ExitValue: " + exitVal);
        } catch (InterruptedException e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
        }
        
    }
	
//	@Test
//	  public void testScalability() throws Exception {
//		System.out.println("testScalability");
//	      PigServer pigServer = new PigServer(pigContext);
//	      pigServer.setBatchOn();
//	      pigServer.registerQuery("A = load '" + widerowX.toString() + "/part-m-00000' using PigStorage() as (name: chararray, x : int, y : int);");
//	      pigServer.registerQuery("B = group A by name;");
//	      pigServer.registerQuery("C = foreach B generate group, SUM(A.x) as xx, SUM(A.y) as yy;");
//	      pigServer.registerQuery("D = filter C by xx > 100 and yy > 100;");
//	      //pigServer.registerQuery("store D into '" +  out.getAbsolutePath() + "';");
//	      Map<Operator, DataBag> derivedData = pigServer.getExamples("D");
//	  
//	      assertTrue(derivedData != null);
//	  }
	
//	@Test
//	public void testScriptL0() throws Exception {
//
//		PigServer pigServer = new PigServer(pigContext);
//		pigServer.setBatchOn();
//		
//	    pigServer.registerQuery("alpha = load '" + power_users_sampleX.toString() + "/part-m-00000' using PigStorage('\u0001') as (name: chararray, phone: chararray, address: chararray, city: chararray, state: chararray, zip: int);");
//		Map<Operator, DataBag> derivedData = pigServer.getExamples("alpha");
//		
//		assertTrue(derivedData != null);
//	}
//	
//	@Test
//	public void testScriptL00() throws Exception {
//
//		PigServer pigServer = new PigServer(pigContext);
//		pigServer.setBatchOn();
//		
//	    pigServer.registerQuery("A = load '" + page_viewsX.toString() + "/part-m-00000' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader() as (user: chararray, action:int, timespent:int, query_term:chararray, ip_addr:long, timestamp:long,estimated_revenue:double, page_info:map[], page_links:bag{t:(p:map[])});");
//		Map<Operator, DataBag> derivedData = pigServer.getExamples("A");
//		
//		assertTrue(derivedData != null);
//	}

	
	//org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
//	@Test
//	  public void testScriptL1() throws Exception {
//		System.out.println("testScriptL1");
//	      PigServer pigServer = new PigServer(pigContext);
//	      pigServer.setBatchOn();
//	      //pigServer.registerJar("/home/kaituo/code/pig3/trunk/pigperf.jar");
//	      pigServer.registerQuery("A = load '" + page_viewsX.toString() + "/part-m-00000' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()  as (user: chararray, action:int, timespent:int, query_term:chararray, ip_addr:long, timestamp:long,estimated_revenue:double, page_info:map[], page_links:bag{t:(p:map[])});");
//	      		//as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);");
//	      pigServer.registerQuery("B = foreach A generate user, action, page_info, flatten(page_links) as page_links;");
//	      		//user, (int)action as action, (map[])page_info as page_info, flatten((bag{tuple(map[])})page_links) as page_links;");
//	      pigServer.registerQuery("C = foreach B generate user, (action == 1 ? page_info#'a' : page_links#'b') as header;");
//	      pigServer.registerQuery("D = group C by user parallel 40;");
//	      pigServer.registerQuery("E = foreach D generate group, COUNT(C) as cnt;");
//	      Map<Operator, DataBag> derivedData = pigServer.getExamples("E");
//	  
//	      assertTrue(derivedData != null);
//	  }
//	
//	@Test
//	public void testScriptL2() throws Exception {
//		System.out.println("testScriptL2");
//
//		PigServer pigServer = new PigServer(pigContext);
//	    pigServer.setBatchOn();
//	    pigServer.registerQuery("A = load '" + page_viewsX.toString() + "/part-m-00000' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader() as (user: chararray, action:int, timespent:int, query_term:chararray, ip_addr:long, timestamp:long,estimated_revenue:double, page_info:map[], page_links:bag{t:(p:map[])});");
//	    pigServer.registerQuery("B = foreach A generate user, estimated_revenue;");
//	    pigServer.registerQuery("alpha = load '" + power_usersX.toString() + "' using PigStorage('\u0001') as (name: chararray, phone: chararray, address: chararray, city: chararray, state: chararray, zip: int);");
//	    pigServer.registerQuery("beta = foreach alpha generate name;");
//	    pigServer.registerQuery("C = join B by user, beta by name using 'replicated' parallel 40;");
//	    
//	    Map<Operator, DataBag> derivedData = pigServer.getExamples("C");
//		  
//		assertTrue(derivedData != null);
//	}
//	
//	@Test
//	public void testScript3() throws Exception {
//		System.out.println("testScript3");
//
//		PigServer pigServer = new PigServer(pigContext);
//	    pigServer.setBatchOn();
//	    pigServer.registerQuery("A = load '" + page_viewsX.toString() + "/part-m-00000' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader() as (user: chararray, action:int, timespent:int, query_term:chararray, ip_addr:long, timestamp:long,estimated_revenue:double, page_info:map[], page_links:bag{t:(p:map[])});");
//	    pigServer.registerQuery("B = foreach A generate user, estimated_revenue;");
//	    pigServer.registerQuery("alpha = load '" + usersX.toString() + "' using PigStorage('\u0001') as (name: chararray, phone: chararray, address: chararray, city: chararray, state: chararray, zip: int);");
//	    pigServer.registerQuery("beta = foreach alpha generate name;");
//	    pigServer.registerQuery("C = join beta by name, B by user parallel 40;");
//	    pigServer.registerQuery("D = group C by $0 parallel 40;");
//	    pigServer.registerQuery("E = foreach D generate group, SUM(C.estimated_revenue);");
//		Map<Operator, DataBag> derivedData = pigServer.getExamples("E");
//			  
//		assertTrue(derivedData != null);
//	}
//	
//	@Test
//	public void testScript4() throws Exception {
//		System.out.println("testScript4");
//
//		PigServer pigServer = new PigServer(pigContext);
//		pigServer.setBatchOn();
//		pigServer
//				.registerQuery("A = load '"
//						+ page_viewsX.toString()
//						+ "/part-m-00000' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader() as (user: chararray, action:int, timespent:int, query_term:chararray, ip_addr:long, timestamp:long,estimated_revenue:double, page_info:map[], page_links:bag{t:(p:map[])});");
//		pigServer.registerQuery("B = foreach A generate user, action;");
//		pigServer.registerQuery("C = group B by user parallel 40;");
//		pigServer
//				.registerQuery("D = foreach C { aleph = B.action; beth = distinct aleph; generate group, COUNT(beth);}");
//		Map<Operator, DataBag> derivedData = pigServer.getExamples("D");
//
//		assertTrue(derivedData != null);
//	}
//	
//	@Test
//	public void testScript5() throws Exception {
//		System.out.println("testScript5");
//
//		PigServer pigServer = new PigServer(pigContext);
//		pigServer.setBatchOn();
//		pigServer
//				.registerQuery("A = load '"
//						+ page_viewsX.toString()
//						+ "/part-m-00000' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader() as (user: chararray, action:int, timespent:int, query_term:chararray, ip_addr:long, timestamp:long,estimated_revenue:double, page_info:map[], page_links:bag{t:(p:map[])});");
//		pigServer.registerQuery("B = foreach A generate user;");
//		pigServer
//				.registerQuery("alpha = load '"
//						+ usersX.toString()
//						+ "'using PigStorage('\u0001') as (name: chararray, phone: chararray, address: chararray, city: chararray, state: chararray, zip: int);");
//		pigServer.registerQuery("beta = foreach alpha generate name;");
//		pigServer
//				.registerQuery("C = cogroup beta by name, B by user parallel 40;");
//		pigServer.registerQuery("D = filter C by COUNT(beta) == 0;");
//		pigServer.registerQuery("E = foreach D generate group;");
//		Map<Operator, DataBag> derivedData = pigServer.getExamples("E");
//
//		assertTrue(derivedData != null);
//	}
//	
//	@Test
//	public void testScript6() throws Exception {
//		System.out.println("testScript6");
//
//		PigServer pigServer = new PigServer(pigContext);
//		pigServer.setBatchOn();
//		pigServer
//				.registerQuery("A = load '"
//						+ page_viewsX.toString()
//						+ "/part-m-00000' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader() as (user: chararray, action:int, timespent:int, query_term:chararray, ip_addr:long, timestamp:long,estimated_revenue:double, page_info:map[], page_links:bag{t:(p:map[])});");
//		pigServer
//				.registerQuery("B = foreach A generate user, action, timespent, query_term, ip_addr, timestamp;");
//		pigServer
//				.registerQuery("C = group B by (user, query_term, ip_addr, timestamp) parallel 40;");
//		pigServer
//				.registerQuery("D = foreach C generate flatten(group), SUM(B.timespent);");
//
//		Map<Operator, DataBag> derivedData = pigServer.getExamples("D");
//
//		assertTrue(derivedData != null);
//	}
//	
//	@Test
//	public void testScript7() throws Exception {
//		System.out.println("testScript7");
//
//		PigServer pigServer = new PigServer(pigContext);
//		pigServer.setBatchOn();
//		pigServer
//		.registerQuery("A = load '"
//				+ page_viewsX.toString()
//				+ "/part-m-00000' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader() as (user: chararray, action:int, timespent:int, query_term:chararray, ip_addr:long, timestamp:long,estimated_revenue:double, page_info:map[], page_links:bag{t:(p:map[])});");
//
//		pigServer.registerQuery("B = foreach A generate user, timestamp;");
//		pigServer.registerQuery("C = group B by user parallel 40;");
//		pigServer.registerQuery("D = foreach C { morning = filter B by timestamp < 43200; afternoon = filter B by timestamp >= 43200; generate group, COUNT(morning), COUNT(afternoon);}");
//		Map<Operator, DataBag> derivedData = pigServer.getExamples("D");
//		
//		assertTrue(derivedData != null);
//	}
//	
//	@Test
//	public void testScript8() throws Exception {
//		System.out.println("testScript8");
//
//		PigServer pigServer = new PigServer(pigContext);
//		pigServer.setBatchOn();
//		pigServer
//				.registerQuery("A = load '"
//						+ page_viewsX.toString()
//						+ "/part-m-00000' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader() as (user: chararray, action:int, timespent:int, query_term:chararray, ip_addr:long, timestamp:long,estimated_revenue:double, page_info:map[], page_links:bag{t:(p:map[])});");
//		pigServer
//				.registerQuery("B = foreach A generate user, timespent, estimated_revenue;");
//		pigServer.registerQuery("C = group B all;");
//		pigServer
//				.registerQuery("D = foreach C generate SUM(B.timespent), AVG(B.estimated_revenue);");
//
//		Map<Operator, DataBag> derivedData = pigServer.getExamples("D");
//
//		assertTrue(derivedData != null);
//	}
//	
//	@Test
//	public void testScript9() throws Exception {
//		System.out.println("testScript9");
//
//		PigServer pigServer = new PigServer(pigContext);
//		pigServer.setBatchOn();
//		pigServer
//				.registerQuery("A = load '"
//						+ page_viewsX.toString()
//						+ "' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader() as (user: chararray, action:int, timespent:int, query_term:chararray, ip_addr:long, timestamp:long,estimated_revenue:double, page_info:map[], page_links:bag{t:(p:map[])});");
//		pigServer.registerQuery("B = order A by query_term parallel 40;");
//		Map<Operator, DataBag> derivedData = pigServer.getExamples("B");
//
//		assertTrue(derivedData != null);
//	}
//	
//	@Test
//	public void testScript10() throws Exception {
//		System.out.println("testScript10");
//
//		PigServer pigServer = new PigServer(pigContext);
//		pigServer.setBatchOn();
//		pigServer
//				.registerQuery("A = load '"
//						+ page_viewsX.toString()
//						+ "' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader() as (user: chararray, action:int, timespent:int, query_term:chararray, ip_addr:long, timestamp:long,estimated_revenue:double, page_info:map[], page_links:bag{t:(p:map[])});");
//		pigServer
//				.registerQuery("B = order A by query_term, estimated_revenue desc, timespent parallel 40;");
//		Map<Operator, DataBag> derivedData = pigServer.getExamples("B");
//
//		assertTrue(derivedData != null);
//	}
//	
//	@Test
//	public void testScript11() throws Exception {
//		System.out.println("testScript11");
//
//		PigServer pigServer = new PigServer(pigContext);
//		pigServer.setBatchOn();
//		pigServer
//				.registerQuery("A = load '"
//						+ page_viewsX.toString()
//						+ "' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader() as (user: chararray, action:int, timespent:int, query_term:chararray, ip_addr:long, timestamp:long,estimated_revenue:double, page_info:map[], page_links:bag{t:(p:map[])});");
//		pigServer.registerQuery("B = foreach A generate user;");
//		pigServer.registerQuery("C = distinct B parallel 40;");
//		pigServer.registerQuery("alpha = load '" + widerowX.toString() + "/part-m-00000' using PigStorage() as (name: chararray, x : int, y : int);");
//		pigServer.registerQuery("beta = foreach alpha generate name;");
//		pigServer.registerQuery("gamma = distinct beta parallel 40;");
//		pigServer.registerQuery("D = union C, gamma;");
//		pigServer.registerQuery("E = distinct D parallel 40;");
//
//		Map<Operator, DataBag> derivedData = pigServer.getExamples("E");
//		
//		assertTrue(derivedData != null);
//	}
//	
//	@Test
//	public void testScriptL12() throws Exception {
//		System.out.println("testScriptL12");
//
//
//		PigServer pigserver = new PigServer(pigContext);
//		pigserver.setBatchOn();
////		String query = "S = load '"
////				+ page_viewsX.toString()
////				+ "' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader() as (user: chararray, action:int, timespent:int, query_term:chararray, ip_addr:long, timestamp:long,estimated_revenue:double, page_info:map[], page_links:bag{t:(p:map[])});";
////		pigserver.registerQuery(query);
////		query = "A = foreach S generate user, action, timespent, query_term, estimated_revenue;";
//		String query = "A = load '" + data12X + "/part-m-00000' using PigStorage() as (user : chararray, action : chararray, timespent : int, query_term : chararray, estimated_revenue : double);";
//		pigserver.registerQuery(query);
//		query = "split A into B if user is not null, alpha if user is null;";
//		pigserver.registerQuery(query);
//		query = "split B into C if query_term is not null, aleph if query_term is null;";
//		pigserver.registerQuery(query);
//		query = "D = group C by user;";
//		pigserver.registerQuery(query);
//		query = "E = foreach D generate group, MAX(C.estimated_revenue);";
//		pigserver.registerQuery(query);
//		query = "beta = group alpha by query_term;";
//		pigserver.registerQuery(query);
//		query = "gamma = foreach beta generate group, SUM(alpha.timespent);";
//		pigserver.registerQuery(query);
//		query = "beth = group aleph by action;";
//		pigserver.registerQuery(query);
//		query = "gimel = foreach beth generate group, COUNT(aleph);";
//		pigserver.registerQuery(query);
//		Map<Operator, DataBag> derivedData = pigserver.getExamples("E");
//		Map<Operator, DataBag> derivedData2 = pigserver.getExamples("gamma");
//		Map<Operator, DataBag> derivedData3 = pigserver.getExamples("gimel");
//		assertTrue(derivedData != null);
//		assertTrue(derivedData2 != null);
//		assertTrue(derivedData3 != null);
//	}
//	
//	@Test
//	public void testScriptL13() throws Exception {
//		System.out.println("testScriptL13");
//
//
//		PigServer pigServer = new PigServer(pigContext);
//		pigServer.setBatchOn();
//		pigServer
//		.registerQuery("A = load '"
//				+ page_viewsX.toString()
//				+ "' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader() as (user: chararray, action:int, timespent:int, query_term:chararray, ip_addr:long, timestamp:long,estimated_revenue:double, page_info:map[], page_links:bag{t:(p:map[])});");
//		pigServer.registerQuery("B = foreach A generate user, estimated_revenue;");
//	    pigServer.registerQuery("alpha = load '" + power_users_sampleX.toString() + "/part-m-00000' using PigStorage('\u0001') as (name: chararray, phone: chararray, address: chararray, city: chararray, state: chararray, zip: int);");
//		pigServer.registerQuery("beta = foreach alpha generate name, phone;");
//		pigServer.registerQuery("C = join B by user left outer, beta by name parallel 40;");
//		Map<Operator, DataBag> derivedData = pigServer.getExamples("C");
//		
//		assertTrue(derivedData != null);
//	}
//	
//	@Test
//	public void testScriptL14() throws Exception {
//		System.out.println("testScriptL14");
//
//
//		PigServer pigServer = new PigServer(pigContext);
//		pigServer.setBatchOn();
//		pigServer
//		.registerQuery("A = load '"
//				+ page_viewsX.toString()
//				+ "' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader() as (user: chararray, action:int, timespent:int, query_term:chararray, ip_addr:long, timestamp:long,estimated_revenue:double, page_info:map[], page_links:bag{t:(p:map[])});");
//		pigServer.registerQuery("B = foreach A generate user, estimated_revenue;");
//	    pigServer.registerQuery("alpha = load '" + users_sortedX.toString() + "/part-r-00000' using PigStorage() as (name: chararray, phone: chararray, address: chararray, city: chararray, state: chararray, zip: int);");
//		pigServer.registerQuery("beta = foreach alpha generate name;");
//		pigServer.registerQuery("C = join B by user, beta by name using 'merge';");
//		Map<Operator, DataBag> derivedData = pigServer.getExamples("C");
//		
//		assertTrue(derivedData != null);
//	}
	
	@Test
	public void testScriptL15() throws Exception {
		System.out.println("testScriptL15");


		PigServer pigServer = new PigServer(pigContext);
		pigServer.setBatchOn();
		pigServer
		.registerQuery("A = load '"
				+ page_viewsX.toString()
				+ "' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader() as (user: chararray, action:int, timespent:int, query_term:chararray, ip_addr:long, timestamp:long,estimated_revenue:double, page_info:map[], page_links:bag{t:(p:map[])});");
		pigServer.registerQuery("B = foreach A generate user, action, estimated_revenue, timespent;");
		pigServer.registerQuery("C = group B by user parallel 40;");
		pigServer.registerQuery("D = foreach C {beth = distinct B.action; rev = distinct B.estimated_revenue; ts = distinct B.timespent; generate group, COUNT(beth), SUM(rev), (int)AVG(ts);}");
		Map<Operator, DataBag> derivedData = pigServer.getExamples2("D");
		
		assertTrue(derivedData != null);
	}
	
//	@Test
//	public void testScriptL16() throws Exception {
//		System.out.println("testScriptL16");
//
//
//		PigServer pigServer = new PigServer(pigContext);
//		pigServer.setBatchOn();
//		pigServer
//				.registerQuery("A = load '"
//						+ page_viewsX.toString()
//						+ "' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader() as (user: chararray, action:int, timespent:int, query_term:chararray, ip_addr:long, timestamp:long,estimated_revenue:double, page_info:map[], page_links:bag{t:(p:map[])});");
//		pigServer
//				.registerQuery("B = foreach A generate user, estimated_revenue;");
//		pigServer.registerQuery("C = group B by user parallel 40;");
//		pigServer
//				.registerQuery("D = foreach C {E = order B by estimated_revenue; F = E.estimated_revenue; generate group, SUM(F);}");
//		Map<Operator, DataBag> derivedData = pigServer.getExamples("D");
//
//		assertTrue(derivedData != null);
//	}
//	
//	@Test
//	public void testScriptL17() throws Exception {
//		System.out.println("testScriptL17");
//
//		PigServer pigserver = new PigServer(pigContext);
//
//		String query = "A = load '"
//				+ widegroupbydataX
//				+ "/part-m-00000' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader() as (user : chararray, action : int, timespent : int, query_term : chararray, ip_addr : long, timestamp : long,"
//				+ "estimated_revenue : double, page_info : map[], page_links : bag{t:(p:map[])}, user_1 : chararray, action_1 : int, timespent_1 : int, query_term_1 : chararray, ip_addr_1 : long, timestamp_1 : long,"
//				+ "estimated_revenue_1 : double, page_info_1 : map[], page_links_1 : bag{t:(p:map[])}, user_2 : chararray, action_2 : int, timespent_2 : int, query_term_2 : chararray, ip_addr_2 : long, timestamp_2 : long,"
//				+ "estimated_revenue_2 : double, page_info_2 : map[], page_links_2 : bag{t:(p:map[])});\n";
//		pigserver.registerQuery(query);
//		System.out.print(query);
//		query = "B = group A by (user, action, timespent, query_term, ip_addr, timestamp,"
//				+ "estimated_revenue, user_1, action_1, timespent_1, query_term_1, ip_addr_1, timestamp_1,"
//				+ "estimated_revenue_1, user_2, action_2, timespent_2, query_term_2, ip_addr_2, timestamp_2,"
//				+ "estimated_revenue_2);\n";
//		pigserver.registerQuery(query);
//		System.out.print(query);
//		query = "C = foreach B generate SUM(A.timespent), SUM(A.timespent_1), SUM(A.timespent_2), AVG(A.estimated_revenue), AVG(A.estimated_revenue_1), AVG(A.estimated_revenue_2);";
//		pigserver.registerQuery(query);
//		System.out.print(query);
//		Map<Operator, DataBag> derivedData = pigserver.getExamples("C");
//		assertTrue(derivedData != null);
//	}

}
