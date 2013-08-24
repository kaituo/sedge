package edu.umass.cs.pig.sort;

import edu.umass.cs.pig.z3.Z3Context;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_sort;

/**
 * 64-bit floating-point type, used for Java double
 * 
 * @author csallner@uta.edu (Christoph Csallner)
 */
public final class Fp64Sort extends Z3SortImpl 
{
  private static final Fp64Sort singleton = new Fp64Sort();
  public static Fp64Sort getInstance() {
    return singleton;
  }  
  
  private Fp64Sort() {
  	// empty
  }
	
  /**
   * FIXME: bogus
   */
	@Override
	protected SWIGTYPE_p__Z3_sort mallocZ3Sort() {
		return Z3Context.get().mk_real_sort();
	}
}
