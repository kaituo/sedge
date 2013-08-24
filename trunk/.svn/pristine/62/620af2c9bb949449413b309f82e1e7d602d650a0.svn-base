package edu.umass.cs.pig.util;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.pen.InvertConstraintVisitor;

import edu.umass.cs.pig.ast.ConstraintResult;
import edu.umass.cs.pig.numeric.CAndCZ3;
import edu.umass.cs.pig.sort.BitVector32Sort;
import edu.umass.cs.pig.sort.BitVector64Sort;
import edu.umass.cs.pig.sort.Fp32Sort;
import edu.umass.cs.pig.sort.Fp64Sort;
import edu.umass.cs.pig.vm.CntrTruth;
import edu.umass.cs.pig.cntr.FakeLimit;
import edu.umass.cs.pig.dataflow.VarInfo;
import edu.umass.cs.pig.z3.Z3Context;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_ast;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_symbol;

public class Allocator4Z3 {
	final Z3Context z3;
	DataByteArrayConverter cvtr;
	CAndCZ3 ccz3;
	
	public Allocator4Z3() {
		super();
		this.z3 = Z3Context.get();
		cvtr = new DataByteArrayConverter();
		ccz3 = new CAndCZ3();
	}
	
	public Allocator4Z3(CAndCZ3 ccz3) {
		super();
		this.z3 = Z3Context.get();
		cvtr = new DataByteArrayConverter();
		this.ccz3 = ccz3;
	}

	public ConstraintResult mallocInZ3(Object value) {
		byte type = DataType.findType(value);
		try {
			return mallocInZ3(value, type);
		} catch (FrontendException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public ConstraintResult mallocInZ3(Object value, byte type) throws FrontendException {
		Allocator4Constant allocConst = new Allocator4Constant();
		return allocConst.mallocInZ3(value, type);
	}
	
	public ConstraintResult mallocInZ3N2(Object value, byte type) throws FrontendException {
		Allocator4Constant allocConst = new Allocator4Constant();
		return allocConst.mallocInZ3N2(value, type);
	}
	
	/**
	 * @author kaituo
	 * @throws FrontendException 
	 */
//	protected String getValueAsZ3String(Object val, byte type) throws FrontendException {
//		//byte type = DataType.findType(val);
//		switch (type) {
//		case DataType.INTEGER:
//			return JavaToZ3.get().getBitVec32(Integer.parseInt(val.toString()));
//		case DataType.LONG:
//			return JavaToZ3.get().getBitVec64(Long.parseLong(val.toString()));
//		case DataType.FLOAT:
//			check(false);
//			return null;
//		case DataType.DOUBLE:
//			check(false);
//			return null;
//		default:
//			throw new FrontendException("Unsupported types.");
//		}
//	}
	
	public ConstraintResult mallocInZ3(String alias, byte type, int lcol) {//, FakeLimit f
//		SWIGTYPE_p__Z3_symbol s;
		SWIGTYPE_p__Z3_ast z = null;
		ConstraintResult ret = null;
//		s = z3.mk_string_symbol(alias);
		FakeLimit f = new FakeLimit();
		
		switch (type) {
		case DataType.INTEGER:
			//z = z3.mk_const(s, BitVector32Sort.getInstance().getZ3Sort());
			z = ccz3.checkCreateInt(alias);
			f.assertFakeLimitInt(z);
			break;
		case DataType.LONG:
//			z = z3.mk_const(s, BitVector64Sort.getInstance().getZ3Sort());
			z = ccz3.checkCreateLong(alias);
//			f.assertFakeLimitIntConversion(z);
			Byte ty = InvertConstraintVisitor.narrowedType.get(alias);
			if (ty != null) {
				// if ty is not null, then this is a value computed by value of another type. 
				// For example, sum (long) = x(int) + y(int)
				f.assertFakeLimitIntConversion(z);
			}
			else
				f.assertFakeLimitLong(z);
			break;
		case DataType.FLOAT:
//			z = z3.mk_const(s, Fp32Sort.getInstance().getZ3Sort());
			z = ccz3.checkCreateFloat(alias);
			break;
		case DataType.DOUBLE:
//			z = z3.mk_const(s, Fp64Sort.getInstance().getZ3Sort());
			z = ccz3.checkCreateDouble(alias);
			break;
		case DataType.CHARARRAY:
		case DataType.BOOLEAN:
			break;
		default:
			return null;
		}
		
         
		ret = new ConstraintResult(z, type, CntrTruth.UNKNOWN, lcol);
		return ret;
	}
	
	public ConstraintResult mallocInZ3(VarInfo vinfo, byte type, int lcol) {
		String alias = vinfo.getAlias();
//		SWIGTYPE_p__Z3_symbol s;
		SWIGTYPE_p__Z3_ast z = null;
		ConstraintResult ret = null;
//		s = z3.mk_string_symbol(alias);
		
		switch (type) {
		case DataType.INTEGER:
//			z = z3.mk_const(s, BitVector32Sort.getInstance().getZ3Sort());
			z = ccz3.checkCreateInt(alias);
			break;
		case DataType.LONG:
//			z = z3.mk_const(s, BitVector64Sort.getInstance().getZ3Sort());
			z = ccz3.checkCreateLong(alias);
			break;
		case DataType.FLOAT:
//			z = z3.mk_const(s, Fp32Sort.getInstance().getZ3Sort());
			z = ccz3.checkCreateFloat(alias);
			break;
		case DataType.DOUBLE:
//			z = z3.mk_const(s, Fp64Sort.getInstance().getZ3Sort());
			z = ccz3.checkCreateDouble(alias);
			break;
		case DataType.CHARARRAY:
		case DataType.BOOLEAN:
			break;
		default:
			return null;
		}
		
         
		ret = new ConstraintResult(z, type, CntrTruth.UNKNOWN, lcol);
		return ret;
	}
	
	public ConstraintResult mallocInZ3(VarInfo vinfo, byte type) {
		String alias = vinfo.getAlias();
//		SWIGTYPE_p__Z3_symbol s;
		SWIGTYPE_p__Z3_ast z = null;
		ConstraintResult ret = null;
//		s = z3.mk_string_symbol(alias);
		
		switch (type) {
		case DataType.INTEGER:
//			z = z3.mk_const(s, BitVector32Sort.getInstance().getZ3Sort());
			z = ccz3.checkCreateInt(alias);
			break;
		case DataType.LONG:
//			z = z3.mk_const(s, BitVector64Sort.getInstance().getZ3Sort());
			z = ccz3.checkCreateLong(alias);
			break;
		case DataType.FLOAT:
//			z = z3.mk_const(s, Fp32Sort.getInstance().getZ3Sort());
			z = ccz3.checkCreateFloat(alias);
			break;
		case DataType.DOUBLE:
//			z = z3.mk_const(s, Fp64Sort.getInstance().getZ3Sort());
			z = ccz3.checkCreateDouble(alias);
			break;
		case DataType.CHARARRAY:
		case DataType.BOOLEAN:
			break;
		default:
			return null;
		}
		
         
		ret = new ConstraintResult(z, type, CntrTruth.UNKNOWN, -1);
		return ret;
	}
	
	public ConstraintResult mallocInZ3(String alias, byte type) {
//		SWIGTYPE_p__Z3_symbol s;
		SWIGTYPE_p__Z3_ast z = null;
		ConstraintResult ret = null;
//		s = z3.mk_string_symbol(alias);
		
		switch (type) {
		case DataType.INTEGER:
//			z = z3.mk_const(s, BitVector32Sort.getInstance().getZ3Sort());
			z = ccz3.checkCreateInt(alias);
			break;
		case DataType.LONG:
//			z = z3.mk_const(s, BitVector64Sort.getInstance().getZ3Sort());
			z = ccz3.checkCreateLong(alias);
			break;
		case DataType.FLOAT:
//			z = z3.mk_const(s, Fp32Sort.getInstance().getZ3Sort());
			z = ccz3.checkCreateFloat(alias);
			break;
		case DataType.DOUBLE:
//			z = z3.mk_const(s, Fp64Sort.getInstance().getZ3Sort());
			z = ccz3.checkCreateDouble(alias);
			break;
		case DataType.CHARARRAY:
		case DataType.BOOLEAN:
			break;
		default:
			return null;
		}
		
         
		ret = new ConstraintResult(z, type, CntrTruth.UNKNOWN, -1);
		return ret;
	}

}
