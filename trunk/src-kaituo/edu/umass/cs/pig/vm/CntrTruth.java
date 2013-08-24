package edu.umass.cs.pig.vm;


public class CntrTruth {
	public static final byte UNKNOWN   =   0;
    public static final byte TRUE      =   1;
    public static final byte FALSE   =   5;
    
    public static byte toCntrTruth(boolean b) {
    	if(b)
    		return CntrTruth.TRUE;
    	else
    		return CntrTruth.FALSE;
    }
}
