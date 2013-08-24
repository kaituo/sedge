package edu.umass.cs.pig.dataflow;

import org.apache.pig.newplan.logical.relational.LogicalSchema;

public class ConstraintArgs {
	Object value;
	VarInfo childVar;
	LogicalSchema schema;
	byte type;
	
	public ConstraintArgs(Object value, VarInfo childVar,
			LogicalSchema schema, byte type) {
		this.schema = schema;
		this.type = type;
		this.value = value;
		this.childVar = childVar;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public VarInfo getChildVar() {
		return childVar;
	}

	public void setChildVar(VarInfo childVar) {
		this.childVar = childVar;
	}

	public LogicalSchema getSchema() {
		return schema;
	}

	public void setSchema(LogicalSchema schema) {
		this.schema = schema;
	}

	public byte getType() {
		return type;
	}

	public void setType(byte type) {
		this.type = type;
	}

	
}
