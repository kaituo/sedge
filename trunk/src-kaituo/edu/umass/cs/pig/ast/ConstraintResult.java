package edu.umass.cs.pig.ast;

import org.apache.pig.data.DataType;

import edu.umass.cs.pig.vm.CntrTruth;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_ast;

public class ConstraintResult {
	private SWIGTYPE_p__Z3_ast z3ast;
	private byte type;
	private byte invert;
	private boolean isConst;
    private Object constVal;
    
    /* used for generating examples for string or boolean constraints. */
    private int col;
	
	public ConstraintResult() {
		this(null, DataType.UNKNOWN, CntrTruth.UNKNOWN);
	}
	
	public ConstraintResult(byte i) {
		this(null, DataType.CHARARRAY, i);
	}
	
	public ConstraintResult(SWIGTYPE_p__Z3_ast z, byte b, byte i,
			boolean isC, Object cstV, int c) {
		z3ast = z;
		type = b;
		invert = i;
		isConst = isC;
		constVal = cstV;
		col = c;
	}
	
	public ConstraintResult(SWIGTYPE_p__Z3_ast z, byte b, byte i) {
		z3ast = z;
		type = b;
		invert = i;
		isConst = false;
		constVal = null;
		col = -1;
	}
	
	public ConstraintResult(SWIGTYPE_p__Z3_ast z, byte b, byte i, int c) {
		z3ast = z;
		type = b;
		invert = i;
		isConst = false;
		constVal = null;
		col = c;
	}

	public SWIGTYPE_p__Z3_ast getZ3ast() {
		return z3ast;
	}

	public void setZ3ast(SWIGTYPE_p__Z3_ast z3ast) {
		this.z3ast = z3ast;
	}

	public byte getType() {
		return type;
	}

	public void setType(byte type) {
		this.type = type;
	}

	public byte isInvert() {
		return invert;
	}

	public void setInvert(byte invert) {
		this.invert = invert;
	}

	public boolean isConst() {
		return isConst;
	}

	public void setConst(boolean isConst) {
		this.isConst = isConst;
	}

	public Object getConstVal() {
		return constVal;
	}

	public void setConstVal(Object constVal) {
		this.constVal = constVal;
	}

	public byte getInvert() {
		return invert;
	}

	public int getCol() {
		return col;
	}

	public void setCol(int col) {
		this.col = col;
	}

	
}
