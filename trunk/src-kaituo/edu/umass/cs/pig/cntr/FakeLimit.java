package edu.umass.cs.pig.cntr;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;

import edu.umass.cs.pig.ast.ConstraintResult;
import edu.umass.cs.pig.util.Allocator4Constant;
import edu.umass.cs.pig.z3.Z3Context;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_ast;

public class FakeLimit implements BackgroundAxiom {
	public static int MAX_INT = 2147483640;
	public static int MIN_INT = -2147483640;
	public static long MAX_LONG = 9223372036854775800L;
	public static long MIN_LONG = -9223372036854775800L;
	
	private Allocator4Constant alloc;
	
//	private List<SWIGTYPE_p__Z3_ast> bounds;
	
	public FakeLimit() {
		alloc = new Allocator4Constant();
//		bounds = new ArrayList<SWIGTYPE_p__Z3_ast>();
	}
	
	public boolean assertFakeLimitIntConversion(SWIGTYPE_p__Z3_ast z3Var) {
		try {
			SWIGTYPE_p__Z3_ast assertMin3;
			SWIGTYPE_p__Z3_ast assertMax3;
			ConstraintResult z3Max2 = alloc.mallocInZ3(MAX_INT, DataType.LONG);
			ConstraintResult z3Min2 = alloc.mallocInZ3(MIN_INT, DataType.LONG);
			assertMin3 = Z3Context.get().mk_bvsgt(z3Var, z3Min2.getZ3ast());
			assertMax3 = Z3Context.get().mk_bvslt(z3Var, z3Max2.getZ3ast());
			Z3Context.get().assert_cnstr(assertMax3);
			Z3Context.get().assert_cnstr(assertMin3);
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
	
	public boolean assertFakeLimitIntConversion2(SWIGTYPE_p__Z3_ast z3Var) {
		try {
			SWIGTYPE_p__Z3_ast assertMin3;
			SWIGTYPE_p__Z3_ast assertMax3;
			ConstraintResult z3Max2 = alloc.mallocInZ3N2(MAX_INT, DataType.LONG);
			ConstraintResult z3Min2 = alloc.mallocInZ3N2(MIN_INT, DataType.LONG);
			assertMin3 = Z3Context.get().mk_bvsgt(z3Var, z3Min2.getZ3ast());
			assertMax3 = Z3Context.get().mk_bvslt(z3Var, z3Max2.getZ3ast());
			Z3Context.get().assert_cnstr(assertMax3);
			Z3Context.get().assert_cnstr(assertMin3);
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
			ConstraintResult z3MinInt;
			SWIGTYPE_p__Z3_ast assertMin;
			SWIGTYPE_p__Z3_ast assertMax;
			z3MaxInt = alloc.mallocInZ3(MAX_INT, DataType.INTEGER);
			z3MinInt = alloc.mallocInZ3(MIN_INT, DataType.INTEGER);
			assertMin = Z3Context.get().mk_bvsgt(z3Var, z3MinInt.getZ3ast());
			assertMax = Z3Context.get().mk_bvslt(z3Var, z3MaxInt.getZ3ast());
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
	
	public boolean assertFakeLimitInt2(SWIGTYPE_p__Z3_ast z3Var) {
		try {
			ConstraintResult z3MaxInt;
			ConstraintResult z3MinInt;
			SWIGTYPE_p__Z3_ast assertMin;
			SWIGTYPE_p__Z3_ast assertMax;
			z3MaxInt = alloc.mallocInZ3N2(MAX_INT, DataType.INTEGER);
			z3MinInt = alloc.mallocInZ3N2(MIN_INT, DataType.INTEGER);
			assertMin = Z3Context.get().mk_bvsgt(z3Var, z3MinInt.getZ3ast());
			assertMax = Z3Context.get().mk_bvslt(z3Var, z3MaxInt.getZ3ast());
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
			ConstraintResult z3MinLong;
			SWIGTYPE_p__Z3_ast assertMin2;
			SWIGTYPE_p__Z3_ast assertMax2;
			z3MaxLong = alloc.mallocInZ3(MAX_LONG, DataType.LONG);
			z3MinLong = alloc.mallocInZ3(MIN_LONG, DataType.LONG);
			assertMin2 = Z3Context.get().mk_bvsgt(z3Var, z3MinLong.getZ3ast());
			assertMax2 = Z3Context.get().mk_bvslt(z3Var, z3MaxLong.getZ3ast());
//			bounds.add(assertMax2);
//			bounds.add(assertMin2);
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
	
	public boolean assertFakeLimitLong2(SWIGTYPE_p__Z3_ast z3Var) {
		try {
			ConstraintResult z3MaxLong;
			ConstraintResult z3MinLong;
			SWIGTYPE_p__Z3_ast assertMin2;
			SWIGTYPE_p__Z3_ast assertMax2;
			z3MaxLong = alloc.mallocInZ3N2(MAX_LONG, DataType.LONG);
			z3MinLong = alloc.mallocInZ3N2(MIN_LONG, DataType.LONG);
			assertMin2 = Z3Context.get().mk_bvsgt(z3Var, z3MinLong.getZ3ast());
			assertMax2 = Z3Context.get().mk_bvslt(z3Var, z3MaxLong.getZ3ast());
//			bounds.add(assertMax2);
//			bounds.add(assertMin2);
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

//	public List<SWIGTYPE_p__Z3_ast> getBounds() {
//		return bounds;
//	}

//	public void setBounds(List<SWIGTYPE_p__Z3_ast> bounds) {
//		this.bounds = bounds;
//	}
//	
//	public void clearBounds() {
//		bounds.clear();
//	}
}
