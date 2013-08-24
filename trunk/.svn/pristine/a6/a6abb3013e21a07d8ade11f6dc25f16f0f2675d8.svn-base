package edu.umass.cs.pig.real;

import edu.umass.cs.pig.MainConfig;
import edu.umass.cs.pig.z3.DisplayModel;
import edu.umass.cs.pig.z3.Z3Context;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_ast;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_model;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_sort;
import edu.umass.cs.z3.Z3_lbool;

public class Check {
	protected final MainConfig conf;
	Z3Context z3;
	
	public Check(MainConfig conf) {
		this.conf = conf;
	}

	public Check() {
		this(MainConfig.setInstance()); // initialize with default param values
		z3 = Z3Context.get();
	}
	
	void check(Z3_lbool expected_result) {
		
	}
	
	void numeral_example() {
		SWIGTYPE_p__Z3_ast n1, n2;
		SWIGTYPE_p__Z3_sort real_ty, int_ty;
		SWIGTYPE_p__Z3_model m;
		System.out.println("\nnumeral_example\n");
		
//		double a = Double.parseDouble("1/2");
//		System.out.println(a);
		
		int_ty = z3.mk_int_sort();
		n1 = z3.mk_numeral("1", int_ty);
		n2 = z3.mk_numeral("-2", int_ty);
		System.out.format("Numerals n1:%s", z3.ast_to_string(n1));
		System.out.format(" n2:%s\n", z3.ast_to_string(n2));
		
		real_ty = z3.mk_real_sort();
		int_ty = z3.mk_int_sort();
		n1 = z3.mk_numeral("1/2", real_ty);
		n2 = z3.mk_numeral("1 1/2", real_ty);
		System.out.format("Numerals n1:%s", z3.ast_to_string(n1));
		System.out.format(" n2:%s\n", z3.ast_to_string(n2));
//		m = prove(z3.mk_eq(n1, n2));
//		z3.model_to_string(m);
//		if (m != null) {
//	        z3.del_model(m);
//	    }

		n1 = z3.mk_numeral("-1/3", real_ty);
		n2 = z3.mk_numeral("-0.333", real_ty);
		System.out.format("Numerals n1:%s", z3.ast_to_string(n1));
		System.out.format(" n2:%s\n", z3.ast_to_string(n2));
//		m = prove(z3.mk_not(z3.mk_eq(n1, n2)));
//		z3.model_to_string(m);
//		if (m != null) {
//	        z3.del_model(m);
//	    }
		
		z3.del_context();
		
	}
	
	/**
	   \brief Prove that the constraints already asserted into the logical
	   context implies the given formula.  The result of the proof is
	   displayed.
	   
	   Z3 is a satisfiability checker. So, one can prove \c f by showing
	   that <tt>(not f)</tt> is unsatisfiable.

	   The context \c ctx is not modified by this function.
	*/
	public SWIGTYPE_p__Z3_model prove(SWIGTYPE_p__Z3_ast f)
	{
		SWIGTYPE_p__Z3_ast   not_f;

	    /* save the current state of the context */
	    z3.push();

	    not_f = z3.mk_not(f);
	    z3.assert_cnstr(not_f);
	    
	    SWIGTYPE_p__Z3_model res = z3.check_and_get_model_simple();
	    
	    

	    /* restore context */
	    z3.pop(1);
	    
	    return res;
	}
	
	/**
	   \brief Find a model for <tt>x < y + 1, x > 2</tt>.
	   Then, assert <tt>not(x = y)</tt>, and find another model.
	*/
	void find_model_example2() 
	{
	    DisplayModel d = new DisplayModel();
		SWIGTYPE_p__Z3_ast x, y, one, two, y_plus_one;
		SWIGTYPE_p__Z3_ast x_eq_y;
		SWIGTYPE_p__Z3_ast[] args = new SWIGTYPE_p__Z3_ast[2];
		SWIGTYPE_p__Z3_ast c1, c2, c3;

	    System.out.print("\nfind_model_example2\n");
	    
	    
	    x          = z3.mk_real_var("x");
	    y          = z3.mk_real_var("y");
	    one        = z3.mk_real("1/3");
	    two        = z3.mk_real("2/3");

	    args[0]    = y;
	    args[1]    = one;
	    y_plus_one = z3.mk_add(2, args);

	    c1         = z3.mk_lt(x, y_plus_one);
	    c2         = z3.mk_gt(x, two);
	    
	    z3.assert_cnstr(c1);
	    z3.assert_cnstr(c2);

	    System.out.print("model for: x < y + 1/3, x > 2/3\n");
	    SWIGTYPE_p__Z3_model m = z3.check_and_get_model_simple();
	    d.display_model(m);

	    /* assert not(x = y) */
	    x_eq_y     = z3.mk_eq(x, y);
	    c3         = z3.mk_not(x_eq_y);
	    z3.assert_cnstr(c3);

	    System.out.print("model for: x < y + 1/3, x > 2/3, not(x = y)\n");
	    m = z3.check_and_get_model_simple();
	    d.display_model(m);

	    z3.del_context();
	}
	
	void find_model_example1() 
	{
	    DisplayModel d = new DisplayModel();
		SWIGTYPE_p__Z3_ast x, y, one, two, y_plus_one;
		SWIGTYPE_p__Z3_ast x_eq_y;
		SWIGTYPE_p__Z3_ast[] args = new SWIGTYPE_p__Z3_ast[2];
		SWIGTYPE_p__Z3_ast c1, c2, c3;

	    System.out.print("\nfind_model_example2\n");
	    
	    
	    x          = z3.mk_real_var("x");
	    y          = z3.mk_real_var("y");
	    one        = z3.mk_real("1/3");
	    two        = z3.mk_real("2/3");

	    args[0]    = y;
	    args[1]    = one;
	    y_plus_one = z3.mk_add(2, args);

	    c1         = z3.mk_lt(x, y_plus_one);
	    c2         = z3.mk_gt(x, two);
	    
	    z3.assert_cnstr(c1);
	    z3.assert_cnstr(c2);

	    System.out.print("model for: x < y + 1/3, x > 2/3\n");
	    SWIGTYPE_p__Z3_model m = z3.check_and_get_model_simple();
	    d.display_model(m);

	    /* assert not(x = y) */
	    x_eq_y     = z3.mk_eq(x, y);
	    c3         = z3.mk_not(x_eq_y);
	    z3.assert_cnstr(c3);

	    System.out.print("model for: x < y + 1/3, x > 2/3, not(x = y)\n");
	    m = z3.check_and_get_model_simple();
	    d.display_model(m);

	    z3.del_context();
	}
	
	public static void main(String[] args) {
		Check chk = new Check();
		//chk.numeral_example();
		chk.find_model_example2();
	}
}
