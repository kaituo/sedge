package edu.umass.cs.pig.ast;

import edu.umass.cs.z3.SWIGTYPE_p__Z3_sort;

/**
 * Z3 sort
 * 
 * <p>("sort" is a similar concept as "type")
 * 
 * @see http://research.microsoft.com/en-us/um/redmond/projects/z3/group__capi.html
 * 
 * @author csallner@uta.edu (Christoph Csallner)
 */
public interface Z3Sort extends Z3Node {
  
  /**
   * Malloc (or retrieve previously malloced) Z3 C pointer
   * representing this sort.
   * 
   * <p>When malloc, then also assert any axioms about this node
   * into the current Z3 context.
   * 
   * @return Z3 C pointer representing this sort. 
   */
	public SWIGTYPE_p__Z3_sort getZ3Sort();
}

