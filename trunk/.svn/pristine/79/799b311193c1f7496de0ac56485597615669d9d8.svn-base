package edu.umass.cs.pig.util;

import java.util.Iterator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;

public class NullAnalyzer {
	public static boolean  containsNull(DataBag bag) throws FrontendException {
		Iterator it = bag.iterator();
        while (it.hasNext()){
            Tuple t = (Tuple)it.next();
            int size = t.size();
            for(int i=0; i<size; i++) {
            	try {
            		if(t.isNull(i)) {
            			return true;
            		}
            	} catch(ExecException e) {
            		throw new FrontendException("the field number given is greater than or equal to the number of fields in the tuple"
            				+ e.getMessage(), e);
            	}
            }
        }
        return false;
	}

}
