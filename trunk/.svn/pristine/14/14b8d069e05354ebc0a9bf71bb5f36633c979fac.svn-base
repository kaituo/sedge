package edu.umass.cs.pig.cntr;

import edu.umass.cs.pig.ast.ConstraintResult;
import edu.umass.cs.pig.z3.Z3Context;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_ast;

public class OverflowUnderFlow implements BackgroundAxiom {
	Z3Context z3 = Z3Context.get();
	
	public void bvmulouflow(ConstraintResult left, ConstraintResult right) {
		SWIGTYPE_p__Z3_ast c2 = z3.mk_bvmul_no_overflow(left.getZ3ast(), right.getZ3ast());
		SWIGTYPE_p__Z3_ast c3 = z3.mk_bvmul_no_underflow(left.getZ3ast(), right.getZ3ast());
		z3.assert_cnstr(c2);
		z3.assert_cnstr(c3);
	}
	
	public void bvsubouflow(ConstraintResult left, ConstraintResult right) {
		SWIGTYPE_p__Z3_ast c2 = z3.mk_bvsub_no_overflow(left.getZ3ast(), right.getZ3ast());
		SWIGTYPE_p__Z3_ast c3 = z3.mk_bvsub_no_underflow(left.getZ3ast(), right.getZ3ast());
		z3.assert_cnstr(c2);
		z3.assert_cnstr(c3);
	}
	
	public void bvaddouflow(ConstraintResult left, ConstraintResult right) {
		SWIGTYPE_p__Z3_ast c2 = z3.mk_bvadd_no_overflow(left.getZ3ast(), right.getZ3ast());
		SWIGTYPE_p__Z3_ast c3 = z3.mk_bvadd_no_underflow(left.getZ3ast(), right.getZ3ast());
		z3.assert_cnstr(c2);
		z3.assert_cnstr(c3);
	}
	
	public void bvdivouflow(ConstraintResult left, ConstraintResult right) {
		SWIGTYPE_p__Z3_ast c2 = z3.mk_bvsdiv_no_overflow(left.getZ3ast(), right.getZ3ast());
		z3.assert_cnstr(c2);
	}

}
