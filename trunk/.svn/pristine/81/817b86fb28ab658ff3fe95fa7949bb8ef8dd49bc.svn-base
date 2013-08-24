package edu.umass.cs.pig.cntr;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;

import edu.umass.cs.pig.ast.ConstraintResult;
import edu.umass.cs.pig.util.Allocator4Constant;
import edu.umass.cs.pig.z3.Z3Context;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_ast;

public class FakeLimit3 {
	public static int MAX_INT = 2147483640;
	public static long MAX_LONG = 9223372036854775800L;
	private Allocator4Constant alloc;
	Z3Context z3;
	
//	private List<SWIGTYPE_p__Z3_ast> bounds;
	
	public FakeLimit3() {
		alloc = new Allocator4Constant();
//		bounds = new ArrayList<SWIGTYPE_p__Z3_ast>();
		z3 = Z3Context.get();
	}
	
	public boolean assertFakeLimitIntConversion(SWIGTYPE_p__Z3_ast z3Var) {
		try {
			SWIGTYPE_p__Z3_ast assertMin3, assertMinHelp3;
			SWIGTYPE_p__Z3_ast assertMax3, assertMaxHelp3;
			ConstraintResult z3Zero = alloc.mallocInZ3(0, DataType.LONG);
			ConstraintResult z3Max2 = alloc.mallocInZ3(MAX_INT, DataType.LONG);
			
			assertMinHelp3 = z3.mk_bvadd(z3Var, z3Max2.getZ3ast());
			SWIGTYPE_p__Z3_ast c2 = z3.mk_bvadd_no_overflow(z3Var, z3Max2.getZ3ast());
			SWIGTYPE_p__Z3_ast c3 = z3.mk_bvadd_no_underflow(z3Var, z3Max2.getZ3ast());
			
			assertMaxHelp3 = z3.mk_bvsub(z3Var, z3Max2.getZ3ast());
			SWIGTYPE_p__Z3_ast c4 = z3.mk_bvsub_no_overflow(z3Var, z3Max2.getZ3ast());
			SWIGTYPE_p__Z3_ast c5 = z3.mk_bvsub_no_underflow(z3Var, z3Max2.getZ3ast());
			
			assertMin3 = z3.mk_bvsge(assertMinHelp3, z3Zero.getZ3ast());
			assertMax3 = z3.mk_bvsle(assertMaxHelp3, z3Zero.getZ3ast());
			
			z3.assert_cnstr(c2);
			z3.assert_cnstr(c3);
			z3.assert_cnstr(c4);
			z3.assert_cnstr(c5);
			
			z3.assert_cnstr(assertMax3);
			z3.assert_cnstr(assertMin3);
			if(assertMin3 != null && assertMax3 != null) 
				return true;
			else
				return false;
		} catch (FrontendException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return false;
	}
	
	public boolean assertFakeLimitInt(SWIGTYPE_p__Z3_ast z3Var) {
		try {
			ConstraintResult z3MaxInt;
			SWIGTYPE_p__Z3_ast assertMin, assertMinHelp;
			SWIGTYPE_p__Z3_ast assertMax, assertMaxHelp;
			ConstraintResult z3Zero = alloc.mallocInZ3(0, DataType.INTEGER);
			z3MaxInt = alloc.mallocInZ3(MAX_INT, DataType.INTEGER);
			
			assertMinHelp = z3.mk_bvadd(z3Var, z3MaxInt.getZ3ast());
			SWIGTYPE_p__Z3_ast c2 = z3.mk_bvadd_no_overflow(z3Var, z3MaxInt.getZ3ast());
			SWIGTYPE_p__Z3_ast c3 = z3.mk_bvadd_no_underflow(z3Var, z3MaxInt.getZ3ast());
			
			assertMaxHelp = z3.mk_bvsub(z3Var, z3MaxInt.getZ3ast());
			SWIGTYPE_p__Z3_ast c4 = z3.mk_bvsub_no_overflow(z3Var, z3MaxInt.getZ3ast());
			SWIGTYPE_p__Z3_ast c5 = z3.mk_bvsub_no_underflow(z3Var, z3MaxInt.getZ3ast());
			
			assertMin = z3.mk_bvsge(assertMinHelp, z3Zero.getZ3ast());
			assertMax = z3.mk_bvsle(assertMaxHelp, z3Zero.getZ3ast());
			
			z3.assert_cnstr(c2);
			z3.assert_cnstr(c3);
			z3.assert_cnstr(c4);
			z3.assert_cnstr(c5);
			
			Z3Context.get().assert_cnstr(assertMax);
			Z3Context.get().assert_cnstr(assertMin);
			if (assertMin != null && assertMax != null)
				return true;
			else
				return false;
		} catch (FrontendException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
	public boolean assertFakeLimitLong(SWIGTYPE_p__Z3_ast z3Var) {
		try {
			ConstraintResult z3MaxLong;
			SWIGTYPE_p__Z3_ast assertMin2, assertMinHelp2;
			SWIGTYPE_p__Z3_ast assertMax2, assertMaxHelp2;
			
			ConstraintResult z3Zero = alloc.mallocInZ3(0, DataType.LONG);
			z3MaxLong = alloc.mallocInZ3(MAX_LONG, DataType.LONG);
			
			assertMinHelp2 = z3.mk_bvadd(z3Var, z3MaxLong.getZ3ast());
			SWIGTYPE_p__Z3_ast c2 = z3.mk_bvadd_no_overflow(z3Var, z3MaxLong.getZ3ast());
			SWIGTYPE_p__Z3_ast c3 = z3.mk_bvadd_no_underflow(z3Var, z3MaxLong.getZ3ast());
			
			assertMaxHelp2 = z3.mk_bvsub(z3Var, z3MaxLong.getZ3ast());
			SWIGTYPE_p__Z3_ast c4 = z3.mk_bvsub_no_overflow(z3Var, z3MaxLong.getZ3ast());
			SWIGTYPE_p__Z3_ast c5 = z3.mk_bvsub_no_underflow(z3Var, z3MaxLong.getZ3ast());
			
			assertMin2 = z3.mk_bvsge(assertMinHelp2, z3Zero.getZ3ast());
			assertMax2 = z3.mk_bvsle(assertMaxHelp2, z3Zero.getZ3ast());
//			bounds.add(assertMax2);
//			bounds.add(assertMin2);
			
			z3.assert_cnstr(c2);
			z3.assert_cnstr(c3);
			z3.assert_cnstr(c4);
			z3.assert_cnstr(c5);
			Z3Context.get().assert_cnstr(assertMax2);
			Z3Context.get().assert_cnstr(assertMin2);
			if (assertMin2 != null && assertMax2 != null)
				return true;
			else
				return false;
		} catch (FrontendException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

}
