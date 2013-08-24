package edu.umass.cs.pig.test.pigmix;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.PigContext;

import edu.umass.cs.pig.test.util.RFile;
import edu.umass.cs.pig.test.util.StreamGobbler;

public class DataGeneration2 {
	static PigContext pigContext = new PigContext(ExecType.LOCAL, new Properties());

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
	
	public DataGeneration2() {
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
	}
	
	private void logErrororOutput(Process proc) {
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
	
	private void writeScalabilityData2(String dataOrig, String dataOut, String PIG_FILE) {
		int n = 31;
		int m = 501;

		String abody = "";
		for (int i = 0; i < m; i++) {
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
		
		String ebody = "";
		for (int i = 0; i < n; i++) {
			if (i > 0)
				ebody = ebody + ", ";
			ebody = ebody + "c" + i;
		}
		
		try {
			PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
	        w.println("A = load '" + dataOrig.toString()
					+ "' using PigStorage('\u0001') as (name: chararray, " + abody + ");");
	        w.println("B = foreach A generate name, " + ebody + ";");
	        w.println("store B into '" + dataOut + "';");
	        w.close();
	        
	        Process pp = Runtime.getRuntime().exec("java -Xmx256m -cp /home/kaituo/Downloads/pig-0.9.2/pig-0.9.2.jar org.apache.pig.Main -x local " + PIG_FILE.toString());
	        logErrororOutput(pp);
		} catch(IOException io) {
			io.printStackTrace();
		} 
		
	}
	
	public static void main(String[] args) {
	    String widerow, widerow2X, pScalability2;
	    
	    pScalability2 =  "/home/kaituo/code/pig3/trunk/scripts/pScalability2.pig";
		
		
		widerow = "/home/kaituo/code/pig3/trunk/PigmixRandomData/100/widerow/widerow10m0";

		widerow2X = "/home/kaituo/code/pig3/trunk/PigmixRandomData/100/widerow2X/scalabilityData0";
		
		
		//RFile.createRFile(pScalability2);
		
		RFile.deleteIfExists(widerow2X);
		
		
		DataGeneration2 d = new DataGeneration2();

		d.writeScalabilityData2(widerow, widerow2X, pScalability2);
	}


}
