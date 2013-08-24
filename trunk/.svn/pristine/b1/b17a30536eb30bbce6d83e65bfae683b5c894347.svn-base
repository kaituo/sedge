package edu.umass.cs.pig.sort;

import edu.umass.cs.pig.z3.Z3Context;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_sort;

/**
 * Z3 bool 
 * 
 * (Z3 bool != Java/Jvm boolean or Bool)
 * 
 * @author csallner@uta.edu (Christoph Csallner)
 */
public final class Z3BoolSort extends Z3SortImpl 
{
  private static final Z3BoolSort singleton = new Z3BoolSort();
  public static Z3BoolSort getInstance() {
    return singleton;
  }  
  
  private Z3BoolSort() {
  	// empty
  }
	
	@Override
	protected SWIGTYPE_p__Z3_sort mallocZ3Sort() {
		return Z3Context.get().mk_bool_sort();
	}
}

