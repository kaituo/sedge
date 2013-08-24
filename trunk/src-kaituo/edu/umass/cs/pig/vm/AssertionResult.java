package edu.umass.cs.pig.vm;

import edu.umass.cs.pig.cntr.CountConstraint;

public class AssertionResult {
	private boolean duplicate;
	private boolean z3declared;
	private CountConstraint cc;
	
	public AssertionResult(boolean b, boolean z, CountConstraint c) {
		duplicate = b;
		z3declared = z;
		cc = c;
	}

	public boolean isDuplicate() {
		return duplicate;
	}

	public void setDuplicate(boolean duplicate) {
		this.duplicate = duplicate;
	}

	public boolean isZ3declared() {
		return z3declared;
	}

	public void setZ3declared(boolean z3declared) {
		this.z3declared = z3declared;
	}

	public CountConstraint getCc() {
		return cc;
	}

	public void setCc(CountConstraint cc) {
		this.cc = cc;
	}
}
