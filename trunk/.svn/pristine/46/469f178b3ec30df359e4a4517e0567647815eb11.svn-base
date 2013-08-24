package gov.nasa.jpf.symbc.numeric.solvers;

import org.apache.pig.data.DataType;

import edu.umass.cs.pig.vm.CntrTruth;

public class ConstraintResultCoral {
	private Object coralast;
	private byte type;
	private byte invert;
	private boolean isConst;
    private Object constVal;
    
    /* used for generating examples for string or boolean constraints. */
    private int col;
	
	public ConstraintResultCoral() {
		this(null, DataType.UNKNOWN, CntrTruth.UNKNOWN);
	}
	
	public ConstraintResultCoral(byte i) {
		this(null, DataType.CHARARRAY, i);
	}
	
	public ConstraintResultCoral(Object z, byte b, byte i,
			boolean isC, Object cstV, int c) {
		coralast = z;
		type = b;
		invert = i;
		isConst = isC;
		constVal = cstV;
		col = c;
	}
	
	public ConstraintResultCoral(Object z, byte b, byte i) {
		coralast = z;
		type = b;
		invert = i;
		isConst = false;
		constVal = null;
		col = -1;
	}
	
	public ConstraintResultCoral(Object z, byte b, byte i, int c) {
		coralast = z;
		type = b;
		invert = i;
		isConst = false;
		constVal = null;
		col = c;
	}
	
	public ConstraintResultCoral(Object c, byte b, boolean isC) {
		coralast = null;
		type = b;
		invert = CntrTruth.UNKNOWN;
		isConst = isC;
		constVal = c;
		col = -1;
	}

	public Object getCoralast() {
		return coralast;
	}

	public void setCoralast(Object z3ast) {
		this.coralast = z3ast;
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

	public static void main(String[] args) {
		byte s = -1;
		if(s == -1)
			System.out.println("true");
	}
}
