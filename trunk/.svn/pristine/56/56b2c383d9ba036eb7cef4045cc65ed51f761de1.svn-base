package edu.umass.cs.pig.util;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintStream;


/**
 * Central log.
 * 
 * Offers convenience methods for logging short lists of strings that
 * do not use the String "+" operator.
 * 
 * FIXME: very primitive
 * 
 * @author csallner@uta.edu (Christoph Csallner)
 */
public abstract class Log 
{

	public final static String NL = System.getProperty("line.separator"); //$NON-NLS-1$
	public final static String FS = System.getProperty("file.separator"); //$NON-NLS-1$
	
	/**
	 * FIXME: need a logging system
	 */
	protected final static PrintStream out = System.out;


  /**
   * Log parameter as p.
   */
  public static void log(int p) {
    out.print(p);
  }
	
  /**
   * Log parameter as p.
   */
	public static void log(String p) {
	  out.print(p);
	}
	
  /**
   * Log parameters as ab.
   */
  public static void log(String a, String b) {
    log(a);
    log(b);
  }
  
  /**
   * Log parameters as abc.
   */
  public static void log(String a, String b, String c) {
    log(a);
    log(b);
    log(c);
  }
  
  /**
   * Log parameters as abcd.
   */
  public static void log(String a, String b, String c, String d) {
    log(a);
    log(b);
    log(c);
    log(d);
  }
  
  /**
   * Log parameters as abcde.
   */
  public static void log(String a, String b, String c, String d, String e) {
    log(a);
    log(b);
    log(c);
    log(d);
    log(e);
  }  

  /**
   * Log newline.
   */
  public static void logln() {
    out.println();
  }

	
  /**
   * Log parameter as p followed by newline.
   */
  public static void logln(int p) {
    out.println(p);
  }
	
	/**
	 * Log stack trace of exception. If it is an exception thrown by the user
	 * program, omit lower part of stack trace that shows Dsc invocation
	 * machinery.
	 */
	public static void logln(Throwable e) {
	  if (e==null)
	    return;

	  log("Aborted with: ");
	  logln(e.toString());

	  StackTraceElement[] trace = e.getStackTrace();
	  if (trace==null)
	    return;

	  for (StackTraceElement ste: trace)
	  {
	    if (ste==null)
	      continue;

	    String className = ste.getClassName();
	    if (className!=null && className.startsWith("edu.uta.cse.dsc.vm.MethodExploration"))
	    {
	      logln("\t.. invoked by Dsc.");
	      break;
	    }

	    log("\tat ");
	    logln(ste.toString());
	  } 
	}

  /**
   * Log parameter as p followed by newline.
   */
  public static void logln(String p) {
    out.println(p);
  }

  /**
   * Log parameters as ab followed by newline.
   */
  public static void logln(String a, String b) {
    log(a,b);
    logln();
  }
  
  /**
   * Log parameters as abc followed by newline.
   */
  public static void logln(String a, String b, String c) {
    log(a,b,c);
    logln();
  }
  
  /**
   * Log parameters as abcd followed by newline.
   */
  public static void logln(String a, String b, String c, String d) {
    log(a,b,c,d);
    logln();
  }

  /**
   * Log parameters as abcde followed by newline.
   */
  public static void logln(String a, String b, String c, String d, String e) {
    log(a,b,c,d,e);
    logln();
  }
	
	public static void logfileIf(boolean doLog, Object o, String fileName)
	{
	  if (! doLog)	// src-util should not depend on src-vm  
	    return;
		
		try{
		  FileWriter fstream = new FileWriter(fileName);
		  BufferedWriter writer = new BufferedWriter(fstream);
		  writer.write(o.toString());
		  writer.close();
		}	
		catch (Exception e){ //Catch exception if any
			System.err.println("File error: " + e.getMessage());
		}
	}
	


  /**
   * Logs parameter, if doLog.
   */
	public static void logIf(boolean doLog, String s) {
	  if (doLog)
	    log(s);
	}

	
  /**
   * Logs newline, if doLog.
   */
  public static void loglnIf(boolean doLog) {
    if (doLog)
      logln();
  }
	
  /**
   * Logs parameter followed by newline, if doLog.
   */
	public static void loglnIf(boolean doLog, String s) {
	  if (doLog)
	    logln(s);
	}
	
  /**
   * Logs parameters as ab followed by newline, if doLog.
   */
  public static void loglnIf(boolean doLog, String a, String b) {
    if (doLog)
      logln(a,b);
  }
   
  /**
   * Logs parameters as abc followed by newline, if doLog.
   */
  public static void loglnIf(boolean doLog, String a, String b, String c) { 
    if (doLog)
      logln(a,b,c);
  }
  
  /**
   * Logs parameters as abcd followed by newline, if doLog.
   */
  public static void loglnIf(boolean doLog, String a, String b, String c, String d) { 
    if (doLog)
      logln(a,b,c,d);
  } 
  
  /**
   * Logs parameters as abcde followed by newline, if doLog.
   */
  public static void loglnIf(boolean doLog, String a, String b, String c, String d, String e) { 
    if (doLog)
      logln(a,b,c,d,e);
  }
}

