package edu.umass.cs.pig.test.util;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

/**
 * @author Kaituo
 */
public class RFile {
	public static void createRFile(String fileName) {
		File f = new File(fileName);
		boolean success = false;

	    if (f.exists()) {
	    	success = deleteRFile(f);
	    	if(!success) 
	    		throw new IllegalArgumentException("Delete: deletion failed");
	    }
	    
	    try {
	        f.createNewFile();
        } catch (IOException e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
        }
	}
	
	public static boolean deleteRFile(File f) {
	    if (!f.canWrite())
	      throw new IllegalArgumentException("Delete: write protected: "
	          + f.getName());

	    // Attempt to delete it
	    // return f.delete();
	    if(!f.isDirectory())
	    	return f.delete();
	    else {
	    	try {
				FileUtils.deleteDirectory(f);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
		    return true;
	    }
	    
	}
	
	public static boolean deleteIfExists(String fileName) {
		File f = new File(fileName);
		boolean success = false;

	    if (f.exists()) {
	    	success = deleteRFile(f);
	    	if(!success) 
	    		return false;
	    }
	    return true;
	}

}

