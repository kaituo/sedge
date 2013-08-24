package edu.umass.cs.pig.dataflow;

import java.io.Serializable;

import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.pen.util.ExampleTuple;

/**
 * Record parent tuple and schema info in a dataflow hierarchy.
 * @author kaituo
 *
 */
public class ParentEncapsulation implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private ExampleTuple t;
	private LogicalSchema lschema;
	
	public ParentEncapsulation(ExampleTuple t, LogicalSchema lschema) {
		super();
		this.t = t;
		this.lschema = lschema;
	}
	
	public ExampleTuple getT() {
		return t;
	}
	
	public void setT(ExampleTuple t) {
		this.t = t;
	}
	public LogicalSchema getLschema() {
		return lschema;
	}
	public void setLschema(LogicalSchema lschema) {
		this.lschema = lschema;
	}
	
	

}
