package edu.umass.cs.pig.sort;

import edu.umass.cs.pig.z3.Z3Context;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_sort;

/**
 * 32-bit floating-point type, used for Java float
 * 
 * @author csallner@uta.edu (Christoph Csallner)
 */
public final class Fp32Sort extends Z3SortImpl 
{
  private static final Fp32Sort singleton = new Fp32Sort();
  public static Fp32Sort getInstance() {
    return singleton;
  }  

  
  private Fp32Sort() {
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
