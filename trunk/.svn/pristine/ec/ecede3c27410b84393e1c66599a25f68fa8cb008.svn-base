package edu.umass.cs.pig.z3.test;

import edu.umass.cs.z3.SWIGTYPE_p__Z3_ast;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_sort;


/**
 * @author csallner@uta.edu (Christoph Csallner)
 */
public class SimpleTest extends Z3PlainTest {
  
//  public void testNatural1() {
//    assertTrue(never(eq (num(0), num(1))));
//  }
//  
//  public void testNatural12() {
//    assertTrue(always(eq (num(2), num(2))));
//  }
//  
//  public void testNatural13() {
//    assertTrue(canBeTrue(eq (num(3), num(3))));
//  }
  
  
//  public void testBv1() {
//    assertTrue(never(eq (bv(4), bv(5))));
//  }
//  
//  public void testBv2() {
//    assertTrue(always(eq (bv(6), bv(6))));
//  }
//  
//  public void testBv3() {
//    assertTrue(canBeTrue(eq (bv(7), bv(7))));
//  }  
  
  /**
  \brief Custom function interpretations pretty printer.
*/
  
//  public void testBitvector1() 
//  {
//	  SWIGTYPE_p__Z3_sort        bv_sort;
//	  SWIGTYPE_p__Z3_ast             x, y, c3, zero, ten, x_minus_ten, c1, c2, thm, conjunct, disjunct;
//
//	  System.out.print("\nbitvector_example1\n");
//      
//     bv_sort   = z3.mk_bv_sort(64);
////	  bv_sort = z3.mk_int_sort();
//      
//      x           = z3.mk_var("x", bv_sort);
//      y           = z3.mk_var("y", bv_sort);
//      zero        = z3.mk_numeral("0", bv_sort);
//      ten         = z3.mk_numeral("100", bv_sort);
//      x_minus_ten = z3.mk_bvsub(x, ten);
//      /* bvsle is signed less than or equal to */
////      c1          = z3.mk_bvsle(x, ten);
//      c2          = z3.mk_bvsle(x, ten);
//      c3          = z3.mk_bvsle(y, ten);
//      disjunct	  = or(c2, c3);
//      
////      thm         = z3.mk_iff(c1, c2);
////      System.out.print("disprove: x - 10 <= 0 IFF x <= 10 for (32-bit) machine integers\n");
////      Check c = new Check();
////      SWIGTYPE_p__Z3_model model = c.prove(thm);
////      display_function_interpretations(model);
//      canBeTrue(disjunct);
//      z3.del_context();
//  }
  
  public void testBitvector2() 
  {
	  SWIGTYPE_p__Z3_sort        bv_sort;
	  SWIGTYPE_p__Z3_ast             x, y, c3, zero, ten, x_minus_ten, c1, c2, thm, conjunct, disjunct;

	  System.out.print("\nbitvector_example1\n");
      
     bv_sort   = z3.mk_bv_sort(64);
//	  bv_sort = z3.mk_int_sort();
      
      x           = z3.mk_var("x", bv_sort);
      y           = z3.mk_var("y", bv_sort);
//      zero        = z3.mk_numeral("0", bv_sort);
      ten         = z3.mk_numeral("9223372036854775909", bv_sort);
      x_minus_ten = z3.mk_bvneg(ten);
      /* bvsle is signed less than or equal to */
//      c1          = z3.mk_bvsle(x, ten);
      c2          = z3.mk_bv2int(x_minus_ten);
      String s	= z3.ast_to_string(c2);
      System.out.println(s);
      display_ast(c2);
      //z3.del_context();
  }
}

