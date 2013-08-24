package edu.umass.cs.pig.sort;

import edu.umass.cs.pig.ast.Z3Sort;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_sort;

/**
 * Z3 sort 
 * 
 * <p>Do not create an un-interpreted Z3 type to represent the Java types or 
 * your Java references. The problem is that you cannot create values of an
 * un-interpreted type. This means you have to create a lot of unnecessary 
 * variables and an associated distinct(variables) constraint.
 * 
 * @author csallner@uta.edu (Christoph Csallner)
 */
public abstract class Z3SortImpl implements Z3Sort 
{	
	private SWIGTYPE_p__Z3_sort cachedSortPointer = null;
	
	/**
	 * Create sort in Z3.
	 */
  protected abstract SWIGTYPE_p__Z3_sort mallocZ3Sort();
  
  @Override
  public SWIGTYPE_p__Z3_sort getZ3Sort() {
    if (cachedSortPointer==null)
      cachedSortPointer = mallocZ3Sort();
  	return cachedSortPointer;
  }
}

