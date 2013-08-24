package edu.umass.cs.pig.vm;

import java.io.Serializable;

import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.relational.LOGenerate;

import edu.umass.cs.pig.cntr.CountConstraint;
import edu.umass.cs.pig.cntr.PltRelCntr;

public class PathBranch implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	LogicalExpressionPlan l;
	boolean invert;
	LOGenerate log;
	boolean isDuplicate;
	PltRelCntr ctr;
	CountConstraint cc;
	
	public PathBranch(LogicalExpressionPlan l, boolean invert) {
		this.l = l;
		this.invert = invert;
		log = null;
		isDuplicate = false;
		ctr = null;
		cc = null;
	}
	
	public PathBranch(LOGenerate log) {
		this.l = null;
		this.invert = false;
		this.log = log;
		isDuplicate = false;
		ctr = null;
		cc = null;
	}
	
	public PathBranch(boolean d) {
		this.l = null;
		this.invert = false;
		log = null;
		isDuplicate = d;
		ctr = null;
		cc = null;
	}
	
	public PathBranch(PltRelCntr e) {
		this.l = null;
		this.invert = false;
		log = null;
		isDuplicate = false;
		ctr = e;
		cc = null;
	}
	
	public PathBranch(CountConstraint e, boolean invert) {
		this.l = null;
		this.invert = invert;
		log = null;
		isDuplicate = false;
		ctr = null;
		cc = e;
	}
	
	public LogicalExpressionPlan getL() {
		return l;
	}
	public void setL(LogicalExpressionPlan l) {
		this.l = l;
	}
	public boolean isInvert() {
		return invert;
	}
	public void setInvert(boolean invert) {
		this.invert = invert;
	}

	public LOGenerate getLog() {
		return log;
	}

	public void setLog(LOGenerate log) {
		this.log = log;
	}

	public boolean isDuplicate() {
		return isDuplicate;
	}

	public void setDuplicate(boolean isDuplicate) {
		this.isDuplicate = isDuplicate;
	}

	public PltRelCntr getCtr() {
		return ctr;
	}

	public void setCtr(PltRelCntr ctr) {
		this.ctr = ctr;
	}

	@Override
	public String toString() {
		return "PathBranch [l=" + l + ", invert=" + invert + ", log=" + log
				+ ", isDuplicate=" + isDuplicate + ", ctr=" + ctr + "]";
	}

	public CountConstraint getCc() {
		return cc;
	}

	public void setCc(CountConstraint cc) {
		this.cc = cc;
	}
}
