package edu.umass.cs.pig.util;

import org.apache.commons.math3.fraction.Fraction;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;

import edu.umass.cs.pig.ast.ConstraintResult;
import edu.umass.cs.pig.sort.BitVector32Sort;
import edu.umass.cs.pig.sort.BitVector64Sort;
import edu.umass.cs.pig.vm.CntrTruth;
import edu.umass.cs.pig.z3.Z3Context;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_ast;

public class Allocator4Constant {
	final Z3Context z3;
	DataByteArrayConverter cvtr;
	
	public Allocator4Constant() {
		super();
		this.z3 = Z3Context.get();
		cvtr = new DataByteArrayConverter();
	}
	
	public ConstraintResult mallocInZ3(Object value, byte type) throws FrontendException {
		SWIGTYPE_p__Z3_ast z = null;
		ConstraintResult ret = null;
		switch (type) {
		case DataType.INTEGER:
//			z = z3.mk_numeral(getValueAsZ3String(value, type),
//					BitVector32Sort.getInstance().getZ3Sort());
			int valueInt;
			if(value instanceof DataByteArray)
				valueInt = cvtr.toInt((DataByteArray)value);
			else
				valueInt = ((Integer)value).intValue();
			z = z3.mk_int(valueInt, 
					BitVector32Sort.getInstance().getZ3Sort());
			break;
		case DataType.LONG:
//			z = z3.mk_numeral(getValueAsZ3String(value, type),
//					BitVector64Sort.getInstance().getZ3Sort());
			Long longVal;
			if (value instanceof Integer) {
				int intVal = ((Integer)value).intValue();
				longVal = Long.valueOf(String.valueOf((intVal)));
			} else if (value instanceof DataByteArray) {
				longVal = cvtr.toLong((DataByteArray)value);
			} else {
				longVal = (Long)value;
			}
			
//			long longVal2;
//			if(longVal < 0) {
//				BigInteger b2 = new BigInteger(Long.toString(longVal));
//				BigInteger b3 = new BigInteger("18446744073709551616");
//				BigInteger b4 = b2.add(b3);
//				z = z3.mk_numeral(b4.toString(),
//						BitVector64Sort.getInstance().getZ3Sort());
//			} else {
//				longVal2 = longVal;
			z = z3.mk_int64(longVal, 
				BitVector64Sort.getInstance().getZ3Sort());
		
			break;
		case DataType.FLOAT:
			float f;
			if (value instanceof DataByteArray)
				f = cvtr.toFloat((DataByteArray)value);
			else
				f = ((Float)value).floatValue();
			Fraction fr = new Fraction(f);
			int num = fr.getNumerator();
			int den = fr.getDenominator();
//			z = z3.mk_numeral(getValueAsZ3String(value, type),
//					Fp32Sort.getInstance().getZ3Sort());
			z = z3.mk_real(num, den); 
			break;
		case DataType.DOUBLE:
			double d;
			if (value instanceof DataByteArray)
				d = cvtr.toDouble((DataByteArray)value);
			else
				d = ((Double)value).doubleValue();
			Fraction fr2 = new Fraction(d);
			int num2 = fr2.getNumerator();
			int den2 = fr2.getDenominator();
//			z = z3.mk_numeral(getValueAsZ3String(value, type),
//					Fp32Sort.getInstance().getZ3Sort());
			z = z3.mk_real(num2, den2); 
			break;
		case DataType.CHARARRAY:
		case DataType.BOOLEAN:
			break;
		default:
			return null;
		}
		ret = new ConstraintResult(z, type, CntrTruth.UNKNOWN, true, value, -1);
		return ret;
	}
	
	public ConstraintResult mallocInZ3N2(Object value, byte type) throws FrontendException {
		SWIGTYPE_p__Z3_ast z = null;
		ConstraintResult ret = null;
		switch (type) {
		case DataType.INTEGER:
//			z = z3.mk_numeral(getValueAsZ3String(value, type),
//					BitVector32Sort.getInstance().getZ3Sort());
			int valueInt;
			if(value instanceof DataByteArray)
				valueInt = cvtr.toInt((DataByteArray)value);
			else
				valueInt = ((Integer)value).intValue();
//			z = z3.mk_int(valueInt, 
//					BitVector32Sort.getInstance().getZ3Sort());
			z = z3.mk_numeral(Integer.toString(valueInt), 
					BitVector32Sort.getInstance().getZ3Sort());
			break;
		case DataType.LONG:
//			z = z3.mk_numeral(getValueAsZ3String(value, type),
//					BitVector64Sort.getInstance().getZ3Sort());
			Long longVal;
			if (value instanceof Integer) {
				int intVal = ((Integer)value).intValue();
				longVal = Long.valueOf(String.valueOf((intVal)));
			} else if (value instanceof DataByteArray) {
				longVal = cvtr.toLong((DataByteArray)value);
			} else {
				longVal = (Long)value;
			}
			
//			long longVal2;
//			if(longVal < 0) {
//				BigInteger b2 = new BigInteger(Long.toString(longVal));
//				BigInteger b3 = new BigInteger("18446744073709551616");
//				BigInteger b4 = b2.add(b3);
//				z = z3.mk_numeral(b4.toString(),
//						BitVector64Sort.getInstance().getZ3Sort());
//			} else {
//				longVal2 = longVal;
//			z = z3.mk_int64(longVal, 
//				BitVector64Sort.getInstance().getZ3Sort());
			z = z3.mk_numeral(Long.toString(longVal), 
					BitVector64Sort.getInstance().getZ3Sort());
		
			break;
		case DataType.FLOAT:
			float f;
			if (value instanceof DataByteArray)
				f = cvtr.toFloat((DataByteArray)value);
			else
				f = ((Float)value).floatValue();
			Fraction fr = new Fraction(f);
			int num = fr.getNumerator();
			int den = fr.getDenominator();
//			z = z3.mk_numeral(getValueAsZ3String(value, type),
//					Fp32Sort.getInstance().getZ3Sort());
			z = z3.mk_real(num, den); 
			break;
		case DataType.DOUBLE:
			double d;
			if (value instanceof DataByteArray)
				d = cvtr.toDouble((DataByteArray)value);
			else
				d = ((Double)value).doubleValue();
			Fraction fr2 = new Fraction(d);
			int num2 = fr2.getNumerator();
			int den2 = fr2.getDenominator();
//			z = z3.mk_numeral(getValueAsZ3String(value, type),
//					Fp32Sort.getInstance().getZ3Sort());
			z = z3.mk_real(num2, den2); 
			break;
		case DataType.CHARARRAY:
		case DataType.BOOLEAN:
			break;
		default:
			return null;
		}
		ret = new ConstraintResult(z, type, CntrTruth.UNKNOWN, true, value, -1);
		return ret;
	}
	
	public ConstraintResult mallocUnknownInZ3(byte type) throws FrontendException {
		SWIGTYPE_p__Z3_ast z = null;
		ConstraintResult ret = null;
		switch (type) {
		case DataType.INTEGER:
			z = z3.mk_int(2147483640, 
					BitVector32Sort.getInstance().getZ3Sort());
			break;
		case DataType.LONG:
			z = z3.mk_int64(9223372036854775800L, 
				BitVector64Sort.getInstance().getZ3Sort());
		
			break;
		case DataType.FLOAT:
			z = z3.mk_real(16777210, 1); 
			break;
		case DataType.DOUBLE:
			z = z3.mk_real(2147483640, 1); 
			break;
		case DataType.CHARARRAY:
		case DataType.BOOLEAN:
			break;
		default:
			return null;
		}
		ret = new ConstraintResult(z, type, CntrTruth.UNKNOWN);
		return ret;
	}

}
