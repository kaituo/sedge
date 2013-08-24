package edu.umass.cs.pig.ast;

import edu.umass.cs.z3.SWIGTYPE_p__Z3_ast;

/**
 * Z3 query (sub-) tree
 * 
 * @see http://research.microsoft.com/en-us/um/redmond/projects/z3/group__capi.html
 * 
 * @author csallner@uta.edu (Christoph Csallner)
 */
public interface Z3Ast extends Z3Node {
	
  /**
   * Malloc (or retrieve previously malloced)
   * Z3 C pointer representing this node.
   * 
   * <p>When malloc, then also assert any axioms about this node
   * into the current Z3 context.
   * 
   * @return Z3 C pointer representing this (sub-) tree.
   */
	public SWIGTYPE_p__Z3_ast getZ3Ast();
	
	
  /**
   * Forget Z3 C pointer representing this node (and this node only).<ul><li>
   *   Does not discard pointers of any child nodes.</li><li>
   *   NOP if C pointer does not exist or was discarded previously.</li></ul>
   *   
   * Call this method when deleting the Z3 context that created this pointer.
   */
  public void discardZ3Pointer(); 	
}
