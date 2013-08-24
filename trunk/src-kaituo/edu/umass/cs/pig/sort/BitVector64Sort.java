package edu.umass.cs.pig.sort;

import edu.umass.cs.pig.z3.Z3Context;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_sort;

/**
 * 64 bit-vector type, used for Java long
 * @see stackoverflow/equivalent of define-fun in Z3 API
 * @see C API for Quantifiers
 * @see Is there an option to pretty-print bit vectors as signed decimals
 */
public final class BitVector64Sort extends Z3SortImpl 
{
  private static final BitVector64Sort singleton = new BitVector64Sort();
  public static BitVector64Sort getInstance() {
    return singleton;
  } 
  
  private BitVector64Sort() {
  	// empty
  }
	
	@Override
	protected SWIGTYPE_p__Z3_sort mallocZ3Sort() {
		return Z3Context.get().mk_bv_sort(64);
	}
}

