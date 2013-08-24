package edu.umass.cs.pig.cntr;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;

/**
 * Number of countUid is equal to countNum
 * @author kaituo
 *
 */
public class CountConstraint implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	long countNum;
	List<String> countAlias;
	
	public CountConstraint(int countNum, LogicalExpressionPlan p) {
		super();
		this.countNum = countNum;
		countAlias = new ArrayList<String>();
	}
	
	public CountConstraint() {
		this.countNum = -1L;
		countAlias = new ArrayList<String>();
	}
	
	public long getCountNum() {
		return countNum;
	}
	
	public void setCountNum(long countNum) {
		this.countNum = countNum;
	}
	
	public List<String> getCountFields() {
		return countAlias;
	}
	
	public void addCountFields(String e) {
		countAlias.add(e);
	}
	
	public void asSolvableCntr() {
		
	}
}
