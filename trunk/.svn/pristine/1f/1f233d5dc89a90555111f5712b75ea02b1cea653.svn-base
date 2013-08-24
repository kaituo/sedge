package edu.umass.cs.pig.sort;

import edu.umass.cs.pig.z3.Z3Context;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_sort;

/**
 * 32 bit-vector type, used for Java int, boolean, short, char, and byte.
 * 
 * @author csallner@uta.edu (Christoph Csallner)
 */
public final class BitVector32Sort extends Z3SortImpl 
{
  private static final BitVector32Sort singleton = new BitVector32Sort();
  public static BitVector32Sort getInstance() {
    return singleton;
  }  
  

  private BitVector32Sort() {
  	// empty
  }
	
	@Override
	protected SWIGTYPE_p__Z3_sort mallocZ3Sort() {
		return Z3Context.get().mk_bv_sort(32);
	}
}

