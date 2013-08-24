package edu.umass.cs.pig.z3.test;

//import edu.umass.cs.pig.ast.bitvector.HashConsingBitVector32Factory;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_ast;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_sort;

/**
 * @author csallner@uta.edu (Christoph Csallner)
 */
public abstract class Z3PlainTest extends Z3Test {
  
  protected SWIGTYPE_p__Z3_sort intType;
  protected SWIGTYPE_p__Z3_sort bv30Type;
  protected SWIGTYPE_p__Z3_sort bv32Type;
	
  @Override
  protected void setUp() throws Exception {
  	super.setUp();  	
  	//z3 = Z3Context.get();
    this.intType  = z3.mk_int_sort();
    this.bv30Type = z3.mk_bv_sort(30);
    this.bv32Type = z3.mk_bv_sort(32);
  }
//  
//  
//  @Override
//  protected void tearDown() throws Exception {
//  	super.tearDown();
//  	z3.del_context();
//  }
  
  /**
   * Get a Java int = a 32-bit bitvector.
   */
//  protected SWIGTYPE_p__Z3_ast bv(int value) {
//  	String string = HashConsingBitVector32Factory.getUnsignedInt(value);
//  	return z3.mk_numeral(string, bv32Type);
//  }
  
  
  /**
   * Get a perfect natural number ("God's integer").
   */
  protected SWIGTYPE_p__Z3_ast num(int value) {
  	return z3.mk_numeral(Integer.toString(value), intType);
  }
}

